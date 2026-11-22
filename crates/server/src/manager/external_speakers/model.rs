//! Secret-bearing persistence types are intentionally neither Debug nor model DTOs.
use super::invalid;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use microcrab_protocol::application::WireError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

const MAX_SPEAKERS: usize = 128;
const MAX_LAYERS: usize = 4096;

#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct SecretKey(String);
impl SecretKey {
    fn custom(value: String) -> Result<Self, WireError> {
        // Length and alphabet are enforceable. No statistical test can prove custom entropy.
        if !(43..=128).contains(&value.len())
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
        {
            return Err(invalid(
                "Custom speaker key requires 43..128 ASCII letters, digits, underscores or hyphens",
            ));
        }
        Ok(Self(value))
    }
    fn generate() -> Self {
        // jwt-simple uses its OS-backed RNG for a 32-byte HMAC key (256 random bits).
        Self(URL_SAFE_NO_PAD.encode(jwt_simple::prelude::HS256Key::generate().to_bytes()))
    }
    pub(super) fn fingerprint(&self) -> String {
        Sha256::digest(self.0.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
    pub(super) fn expose(&self) -> &str {
        &self.0
    }
    fn matches(&self, candidate: &str) -> bool {
        // Compare fixed-size digests without an early exit on differing secret bytes.
        let expected = Sha256::digest(self.0.as_bytes());
        let actual = Sha256::digest(candidate.as_bytes());
        expected
            .iter()
            .zip(actual.iter())
            .fold(0u8, |diff, (a, b)| diff | (a ^ b))
            == 0
    }
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum KeyChange {
    Generated,
    Custom { value: String },
    Keep,
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum SpeakerChange {
    Set { key: KeyChange },
    Disable,
}
/// Random proof of a durable credential incarnation, not derived from the bearer or UI fingerprint.
/// Only the private configuration schema serializes this type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Witness([u8; 32]);
impl Witness {
    fn mint() -> Self {
        Self(
            jwt_simple::prelude::HS256Key::generate()
                .to_bytes()
                .try_into()
                .expect("HS256 uses 32-byte random keys"),
        )
    }
}
impl Serialize for Witness {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&URL_SAFE_NO_PAD.encode(self.0))
    }
}
impl<'de> Deserialize<'de> for Witness {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        if encoded.len() != 43 {
            return Err(serde::de::Error::custom(
                "Invalid credential witness encoding",
            ));
        }
        URL_SAFE_NO_PAD
            .decode(encoded)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .map(Self)
            .ok_or_else(|| serde::de::Error::custom("Invalid credential witness encoding"))
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Binding {
    Set {
        key: SecretKey,
        // Legacy origins have no witness until the first witnessed admission persists one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        witness: Option<Witness>,
    },
    Disable,
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct Layer(BTreeMap<String, Binding>);
impl Layer {
    pub(super) fn from_changes(
        changes: BTreeMap<String, SpeakerChange>,
        session: bool,
        previous: Option<&Layer>,
    ) -> Result<Self, WireError> {
        if changes.len() > MAX_SPEAKERS {
            return Err(invalid("External speaker layer exceeds 128 identities"));
        }
        let mut bindings = BTreeMap::new();
        for (identity, change) in changes {
            validate_identity(&identity)?;
            let binding = match change {
                SpeakerChange::Set { key } => match key {
                    KeyChange::Generated => Binding::Set {
                        key: SecretKey::generate(),
                        witness: Some(Witness::mint()),
                    },
                    KeyChange::Custom { value } => Binding::Set {
                        key: SecretKey::custom(value)?,
                        witness: Some(Witness::mint()),
                    },
                    KeyChange::Keep => match previous.and_then(|layer| layer.0.get(&identity)) {
                        Some(binding @ Binding::Set { .. }) => binding.clone(),
                        _ => {
                            return Err(invalid(
                                "Keep requires an existing key in this configuration layer",
                            ));
                        }
                    },
                },
                SpeakerChange::Disable if session => Binding::Disable,
                SpeakerChange::Disable => {
                    return Err(invalid(
                        "Disable is only valid in a sparse Session layer; omit a project identity to remove it",
                    ));
                }
            };
            bindings.insert(identity, binding);
        }
        Ok(Self(bindings))
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SessionLayer {
    pub(super) project_id: Option<String>,
    pub(super) speakers: Layer,
    // Only currently inherited origins that were previously shadowed in this Session.
    // Cleared when that origin disappears/rotates or this Session shadows it again.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(super) inherited_rebinds: BTreeMap<String, Witness>,
}
#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Configuration {
    pub(super) projects: BTreeMap<String, Layer>,
    pub(super) sessions: BTreeMap<String, SessionLayer>,
    #[serde(default)]
    pub(super) operations: BTreeMap<String, Operation>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Operation {
    pub(super) project_id: Option<String>,
    pub(super) session_id: Option<String>,
    pub(super) request_fingerprint: String,
    pub(super) result: super::View,
}
#[derive(Clone, Serialize, PartialEq, Eq, Debug)]
pub(crate) struct SpeakerIdentity {
    pub(crate) identity: String,
    pub(crate) fingerprint: String,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Source {
    Project,
    Session,
}
pub(super) struct Effective<'a> {
    pub(super) key: &'a SecretKey,
    source: Source,
    witness: Option<&'a Witness>,
}

/// In-process bearer proof. Never put this in a response DTO, log, or prompt state.
pub(crate) struct SpeakerCredential {
    pub(crate) identity: String,
    witness: Witness,
}
impl SpeakerCredential {
    pub(crate) fn witness(&self) -> &[u8; 32] {
        &self.witness.0
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(super) enum SpeakerOverride {
    Set { key: RetainedKey },
    Disable,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum RetainedKey {
    Keep,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct SpeakerProjection {
    identity: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    fingerprint: Option<String>,
    source: Source,
    disabled: bool,
}
pub(super) fn validate_identity(value: &str) -> Result<(), WireError> {
    let mut bytes = value.bytes();
    if value.len() > 64
        || !bytes
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        || !bytes.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    {
        return Err(invalid(
            "Speaker identity must match [A-Za-z_][A-Za-z0-9_-]{0,63}",
        ));
    }
    Ok(())
}
fn inconsistent_witnesses() -> WireError {
    WireError::new(
        "configuration_unavailable",
        "Stored external speaker witnesses are not distinct",
    )
}

impl Configuration {
    pub(super) fn replace_project_layer(&mut self, project: String, speakers: Layer) {
        // A replaced project origin already starts with a new witness in every inheriting
        // Session. Do not let an old Session rebind override that new incarnation.
        let previous = self.projects.insert(project.clone(), speakers);
        let current = &self.projects[&project];
        for local in self.sessions.values_mut() {
            if local.project_id.as_ref() != Some(&project) {
                continue;
            }
            local.inherited_rebinds.retain(|identity, _| {
                matches!(
                    (previous.as_ref().and_then(|layer| layer.0.get(identity)), current.0.get(identity)),
                    (Some(Binding::Set { witness: Some(old), .. }), Some(Binding::Set { witness: Some(new), .. })) if old == new
                )
            });
        }
    }

    pub(super) fn replace_session_layer(
        &mut self,
        project_id: Option<String>,
        session: String,
        speakers: Layer,
    ) {
        let mut inherited_rebinds = BTreeMap::new();
        if let Some(previous) = self.sessions.remove(&session)
            && previous.project_id == project_id
        {
            inherited_rebinds = previous.inherited_rebinds;
            inherited_rebinds.retain(|identity, _| !speakers.0.contains_key(identity));
            if let Some(project) = project_id.as_ref().and_then(|id| self.projects.get(id)) {
                for identity in previous.speakers.0.keys() {
                    if !speakers.0.contains_key(identity)
                        && matches!(
                            project.0.get(identity),
                            Some(Binding::Set {
                                witness: Some(_),
                                ..
                            })
                        )
                    {
                        // Removing a disable or override resumes using the project bearer,
                        // but must not revive this Session's former inherited access.
                        inherited_rebinds.insert(identity.clone(), Witness::mint());
                    }
                }
            }
        }
        self.sessions.insert(
            session,
            SessionLayer {
                project_id,
                speakers,
                inherited_rebinds,
            },
        );
    }

    pub(super) fn layer(&self, project: Option<&str>, session: Option<&str>) -> Option<&Layer> {
        if let Some(session) = session {
            self.sessions.get(session).map(|local| &local.speakers)
        } else {
            project.and_then(|project| self.projects.get(project))
        }
    }
    pub(super) fn overrides(
        &self,
        project: Option<&str>,
        session: Option<&str>,
    ) -> BTreeMap<String, SpeakerOverride> {
        self.layer(project, session)
            .map(|layer| {
                layer
                    .0
                    .iter()
                    .map(|(identity, binding)| {
                        (
                            identity.clone(),
                            match binding {
                                Binding::Set { .. } => SpeakerOverride::Set {
                                    key: RetainedKey::Keep,
                                },
                                Binding::Disable => SpeakerOverride::Disable,
                            },
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
    pub(super) fn effective<'a>(
        &'a self,
        project: Option<&str>,
        session: Option<&str>,
    ) -> Result<BTreeMap<String, Effective<'a>>, WireError> {
        let mut result = BTreeMap::new();
        if let Some(layer) = project.and_then(|id| self.projects.get(id)) {
            for (identity, binding) in &layer.0 {
                if let Binding::Set { key, witness } = binding {
                    result.insert(
                        identity.clone(),
                        Effective {
                            key,
                            source: Source::Project,
                            witness: witness.as_ref(),
                        },
                    );
                }
            }
        }
        if let Some(local) = session.and_then(|id| self.sessions.get(id)) {
            if local.project_id.as_deref() != project {
                return Err(WireError::new(
                    "configuration_unavailable",
                    "External speaker Session project binding changed",
                ));
            }
            for (identity, witness) in &local.inherited_rebinds {
                if let Some(inherited) = result.get_mut(identity) {
                    inherited.witness = Some(witness);
                }
            }
            for (identity, binding) in &local.speakers.0 {
                match binding {
                    Binding::Set { key, witness } => {
                        result.insert(
                            identity.clone(),
                            Effective {
                                key,
                                source: Source::Session,
                                witness: witness.as_ref(),
                            },
                        );
                    }
                    Binding::Disable => {
                        result.remove(identity);
                    }
                }
            }
        }
        if result.len() > MAX_SPEAKERS {
            return Err(invalid("Effective external speakers exceed 128 identities"));
        }
        let mut keys = BTreeSet::new();
        for speaker in result.values() {
            if !keys.insert(speaker.key.fingerprint()) {
                return Err(invalid(
                    "Duplicate key in effective external speaker bindings",
                ));
            }
        }
        Ok(result)
    }
    pub(super) fn project(
        &self,
        project: Option<&str>,
        session: Option<&str>,
    ) -> Result<Vec<SpeakerProjection>, WireError> {
        let mut projections: BTreeMap<_, _> = self
            .effective(project, session)?
            .into_iter()
            .map(|(identity, speaker)| {
                (
                    identity.clone(),
                    SpeakerProjection {
                        identity,
                        fingerprint: Some(speaker.key.fingerprint()),
                        source: speaker.source,
                        disabled: false,
                    },
                )
            })
            .collect();
        if let Some(local) = session.and_then(|id| self.sessions.get(id)) {
            for (identity, binding) in &local.speakers.0 {
                if matches!(binding, Binding::Disable) {
                    projections.insert(
                        identity.clone(),
                        SpeakerProjection {
                            identity: identity.clone(),
                            fingerprint: None,
                            source: Source::Session,
                            disabled: true,
                        },
                    );
                }
            }
        }
        Ok(projections.into_values().collect())
    }
    pub(super) fn authenticate(
        &self,
        project: Option<&str>,
        session: &str,
        bearer: &str,
    ) -> Result<SpeakerIdentity, WireError> {
        self.match_bearer(project, session, bearer)
            .map(|(identity, speaker)| SpeakerIdentity {
                identity,
                fingerprint: speaker.key.fingerprint(),
            })
    }

    fn match_bearer(
        &self,
        project: Option<&str>,
        session: &str,
        bearer: &str,
    ) -> Result<(String, Effective<'_>), WireError> {
        if (43..=128).contains(&bearer.len()) {
            let effective = self.effective(project, Some(session))?;
            let mut matched = None;
            for (identity, speaker) in effective {
                if speaker.key.matches(bearer) {
                    matched = Some((identity, speaker));
                }
            }
            if let Some(identity) = matched {
                return Ok(identity);
            }
        }
        Err(WireError::new(
            "unauthenticated",
            "External speaker key is not bound to this Session",
        ))
    }
    pub(super) fn authenticate_witnessed(
        &mut self,
        project: Option<&str>,
        session: &str,
        bearer: &str,
        allow_migration: bool,
    ) -> Result<Option<(SpeakerCredential, bool)>, WireError> {
        let (identity, matched) = self.match_bearer(project, session, bearer)?;
        if let Some(witness) = matched.witness {
            return Ok(Some((
                SpeakerCredential {
                    identity,
                    witness: witness.clone(),
                },
                false,
            )));
        }
        if !allow_migration {
            return Ok(None);
        }
        // Migrate the single authenticated origin; an inherited legacy key is still one
        // project origin, not a bearer copy (or a per-Session migration).
        let layer = match matched.source {
            Source::Project => project.and_then(|id| self.projects.get_mut(id)),
            Source::Session => self
                .sessions
                .get_mut(session)
                .map(|local| &mut local.speakers),
        };
        let Some(Binding::Set {
            witness: stored, ..
        }) = layer.and_then(|layer| layer.0.get_mut(&identity))
        else {
            return Err(WireError::new(
                "configuration_unavailable",
                "External speaker origin is unavailable",
            ));
        };
        let witness = Witness::mint();
        *stored = Some(witness.clone());
        Ok(Some((SpeakerCredential { identity, witness }, true)))
    }

    pub(super) fn validate(&self) -> Result<(), WireError> {
        if self.projects.len() + self.sessions.len() > MAX_LAYERS {
            return Err(invalid(
                "External speaker resource exceeds 4096 configuration layers",
            ));
        }
        // A project key may be inherited by many Sessions, but each stored key has one origin.
        // This prevents a Session custom key from becoming a cross-Session credential.
        let mut origins = BTreeSet::new();
        let mut witnesses = BTreeSet::new();
        for layer in self
            .projects
            .values()
            .chain(self.sessions.values().map(|local| &local.speakers))
        {
            if layer.0.len() > MAX_SPEAKERS {
                return Err(invalid("External speaker layer exceeds 128 identities"));
            }
            for (identity, binding) in &layer.0 {
                validate_identity(identity)?;
                if let Binding::Set { key, witness } = binding {
                    if let Some(witness) = witness
                        && !witnesses.insert(witness)
                    {
                        return Err(inconsistent_witnesses());
                    }
                    SecretKey::custom(key.0.clone())?;
                    if !origins.insert(key.fingerprint()) {
                        return Err(invalid(
                            "Duplicate external speaker key origin; inherit the project binding instead",
                        ));
                    }
                }
            }
        }
        for project in self.projects.keys() {
            self.effective(Some(project), None)?;
        }
        for (session, local) in &self.sessions {
            let project = local
                .project_id
                .as_ref()
                .and_then(|id| self.projects.get(id));
            for (identity, witness) in &local.inherited_rebinds {
                if !witnesses.insert(witness) {
                    return Err(inconsistent_witnesses());
                }
                if local.speakers.0.contains_key(identity)
                    || !matches!(
                        project.and_then(|layer| layer.0.get(identity)),
                        Some(Binding::Set {
                            witness: Some(_),
                            ..
                        })
                    )
                {
                    return Err(WireError::new(
                        "configuration_unavailable",
                        "Stored external speaker inheritance is inconsistent",
                    ));
                }
            }
            self.effective(local.project_id.as_deref(), Some(session))?;
        }
        Ok(())
    }
}
