//! Digest lookup and owner management use separate indexes over one authority.
use super::{
    Change, Credential, KeyError, KeyMetadata, MAX_KEYS_PER_OWNER, MAX_RECORD_BYTES, decode, encode,
};
use rusqlite::{Connection, OptionalExtension as _, TransactionBehavior, params};
use std::{path::Path, time::Duration};

pub(super) struct Store(Connection);

// Select NULL for a corrupt oversized payload without allocating it in Rust.
const PAYLOAD: &str = "CASE WHEN length(credential) <= ?1 THEN credential END";

impl Store {
    pub(super) fn open(data_dir: &Path) -> Result<Self, KeyError> {
        let connection = Connection::open(data_dir.join("storage.sqlite"))?;
        connection.busy_timeout(Duration::from_secs(2))?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS relay_keys (
                digest BLOB NOT NULL PRIMARY KEY CHECK(length(digest) = 32),
                owner TEXT NOT NULL,
                key_id TEXT NOT NULL,
                credential BLOB NOT NULL,
                UNIQUE(owner, key_id)
            ) WITHOUT ROWID;",
        )?;
        Ok(Self(connection))
    }

    pub(super) fn lookup(&self, hash: &[u8; 32]) -> Result<Option<Credential>, KeyError> {
        self.0
            .query_row(
                &format!("SELECT owner, key_id, {PAYLOAD} FROM relay_keys WHERE digest = ?2"),
                params![MAX_RECORD_BYTES, hash.as_slice()],
                row_fields,
            )
            .optional()?
            .map(checked_record)
            .transpose()
    }

    pub(super) fn list(&self, owner: &str) -> Result<Vec<KeyMetadata>, KeyError> {
        let mut query = self.0.prepare(&format!(
            "SELECT owner, key_id, {PAYLOAD} FROM relay_keys WHERE owner = ?2 LIMIT ?3"
        ))?;
        let mut rows = query.query(params![MAX_RECORD_BYTES, owner, MAX_KEYS_PER_OWNER + 1])?;
        let mut keys = Vec::new();
        while let Some(row) = rows.next()? {
            if keys.len() == MAX_KEYS_PER_OWNER {
                return Err(KeyError::Storage("oversized owner index"));
            }
            keys.push(checked_record(row_fields(row)?)?.key);
        }
        Ok(keys)
    }

    pub(super) fn create(
        &mut self,
        hash: &[u8; 32],
        credential: &Credential,
    ) -> Result<(), KeyError> {
        let transaction = self
            .0
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let at_capacity = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM relay_keys WHERE owner = ?1 LIMIT 1 OFFSET ?2)",
            params![&credential.owner, MAX_KEYS_PER_OWNER - 1],
            |row| row.get(0),
        )?;
        if at_capacity {
            return Err(KeyError::Capacity);
        }
        transaction.execute(
            "INSERT INTO relay_keys(digest, owner, key_id, credential) VALUES (?1, ?2, ?3, ?4)",
            params![
                hash.as_slice(),
                &credential.owner,
                &credential.key.id,
                encode(credential)?
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(super) fn change(
        &mut self,
        owner: &str,
        id: &str,
        change: Change,
    ) -> Result<KeyMetadata, KeyError> {
        let transaction = self
            .0
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = transaction.query_row(
            &format!("SELECT owner, key_id, {PAYLOAD} FROM relay_keys WHERE owner = ?2 AND key_id = ?3"),
            params![MAX_RECORD_BYTES, owner, id],
            row_fields,
        ).optional()?.ok_or(KeyError::NotFound)?;
        let mut credential = checked_record(row)?;
        change.apply(&mut credential.key)?;
        if transaction.execute(
            "UPDATE relay_keys SET credential = ?1 WHERE owner = ?2 AND key_id = ?3",
            params![encode(&credential)?, owner, id],
        )? != 1
        {
            return Err(KeyError::Storage("expected exactly one row to change"));
        }
        transaction.commit()?;
        Ok(credential.key)
    }

    pub(super) fn record_use(
        &mut self,
        owner: &str,
        id: &str,
        used_at_ms: u64,
    ) -> Result<(), KeyError> {
        let transaction = self
            .0
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(row) = transaction
            .query_row(
                &format!("SELECT owner, key_id, {PAYLOAD} FROM relay_keys WHERE owner = ?2 AND key_id = ?3"),
                params![MAX_RECORD_BYTES, owner, id],
                row_fields,
            )
            .optional()?
        else {
            return Ok(());
        };
        let mut credential = checked_record(row)?;
        credential.key.last_used_at_ms = Some(
            credential
                .key
                .last_used_at_ms
                .map_or(used_at_ms, |prev| prev.max(used_at_ms)),
        );
        credential.key.request_count = credential.key.request_count.saturating_add(1);
        transaction.execute(
            "UPDATE relay_keys SET credential = ?1 WHERE owner = ?2 AND key_id = ?3",
            params![encode(&credential)?, owner, id],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

struct RowFields {
    owner: String,
    id: String,
    payload: Option<Vec<u8>>,
}
fn row_fields(row: &rusqlite::Row<'_>) -> rusqlite::Result<RowFields> {
    Ok(RowFields {
        owner: row.get(0)?,
        id: row.get(1)?,
        payload: row.get(2)?,
    })
}
fn checked_record(row: RowFields) -> Result<Credential, KeyError> {
    let credential = decode(
        row.payload
            .as_deref()
            .ok_or(KeyError::Storage("oversized SQLite record"))?,
    )?;
    if row.owner != credential.owner || row.id != credential.key.id {
        return Err(KeyError::Storage(
            "SQLite payload does not match indexed identity",
        ));
    }
    Ok(credential)
}
