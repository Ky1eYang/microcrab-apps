use super::{CAPACITY, Counters, DAY_MS, Event, RETENTION_MS, Source, store::Snapshot};
use microcrab_creator::usage::ReportedCacheUsage;
use microcrab_session::Error;
use microcrab_transport::model_metadata::{MetadataSnapshot, ModelMetadata, ModelPricing};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use uuid::Uuid;

const HOUR_MS: u64 = DAY_MS / 24;
pub(crate) const CSV_LIMIT: usize = 1000;

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
pub(crate) enum Range {
    #[serde(rename = "24h")]
    Hours24,
    #[default]
    #[serde(rename = "7d")]
    Days7,
    #[serde(rename = "30d")]
    Days30,
}
impl Range {
    fn count(self) -> u64 {
        match self {
            Self::Hours24 => 24,
            Self::Days7 => 7,
            Self::Days30 => 30,
        }
    }
    fn unit_ms(self) -> u64 {
        match self {
            Self::Hours24 => HOUR_MS,
            _ => DAY_MS,
        }
    }
}

/// A browser supplies the actual start instants, so repeated DST hours remain
/// distinct and 23/25-hour dates or non-hour offsets require no server guess.
#[derive(Serialize)]
pub(crate) struct Window {
    range: Range,
    ends_at_ms: u64,
    bucket_starts_ms: Vec<u64>,
    today_start_ms: u64,
    time_basis: &'static str,
}
impl Window {
    pub(crate) fn new(
        range: Range,
        now: u64,
        starts: Option<&str>,
        today: Option<u64>,
    ) -> Result<Self, &'static str> {
        let unit = range.unit_ms();
        let count = range.count();
        let utc_now = now - now % unit;
        let (bucket_starts_ms, today_start_ms, time_basis) = match (starts, today) {
            (None, None) => (
                (0..count)
                    .rev()
                    .map(|i| utc_now.saturating_sub(i * unit))
                    .collect(),
                now - now % DAY_MS,
                "utc",
            ),
            (Some(starts), Some(today)) => {
                if starts.len() > 510 {
                    return Err("Too many bucket boundaries");
                }
                let values: Vec<u64> = starts
                    .split(',')
                    .map(str::parse)
                    .collect::<Result<_, _>>()
                    .map_err(|_| "Invalid bucket boundary")?;
                let (min_step, max_step) = if unit == HOUR_MS {
                    (HOUR_MS / 2, HOUR_MS * 2)
                } else {
                    (DAY_MS - 3 * HOUR_MS, DAY_MS + 3 * HOUR_MS)
                };
                if values.len() as u64 != count
                    || values.windows(2).any(|p| {
                        !p[1]
                            .checked_sub(p[0])
                            .is_some_and(|step| (min_step..=max_step).contains(&step))
                    })
                    || !values.last().is_some_and(|last| {
                        now.checked_sub(*last)
                            .is_some_and(|elapsed| elapsed <= max_step)
                    })
                    || !now
                        .checked_sub(today)
                        .is_some_and(|elapsed| elapsed <= DAY_MS + 3 * HOUR_MS)
                {
                    return Err("Bucket boundaries are outside the requested window");
                }
                (values, today, "browser")
            }
            _ => return Err("Provide both bucket_starts_ms and today_start_ms"),
        };
        Ok(Self {
            range,
            ends_at_ms: now,
            bucket_starts_ms,
            today_start_ms,
            time_basis,
        })
    }
    fn contains(&self, ts: u64) -> bool {
        ts >= self.bucket_starts_ms[0] && ts <= self.ends_at_ms
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Filter {
    #[serde(default)]
    range: Range,
    #[serde(default)]
    source: SourceFilter,
    bucket_starts_ms: Option<String>,
    today_start_ms: Option<u64>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    model: Option<String>,
    status: Option<String>,
    q: Option<String>,
    pub low_cache_hit_warn_pct: Option<u8>,
}
#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum SourceFilter {
    #[default]
    All,
    Session,
    Relay,
}
impl Filter {
    pub(crate) fn window(&self) -> Result<Window, &'static str> {
        self.validate()?;
        Window::new(
            self.range,
            super::super::now(),
            self.bucket_starts_ms.as_deref(),
            self.today_start_ms,
        )
    }

    fn validate(&self) -> Result<(), &'static str> {
        if self.limit.is_some_and(|limit| !(1..=200).contains(&limit)) {
            return Err("Limit must be between 1 and 200");
        }
        if self.q.as_ref().is_some_and(|q| q.len() > 256)
            || self.model.as_ref().is_some_and(|v| v.len() > 256)
        {
            return Err("Filter exceeds 256 bytes");
        }
        if self
            .low_cache_hit_warn_pct
            .is_some_and(|v| !(1..=99).contains(&v))
        {
            return Err("Cache threshold must be between 1 and 99");
        }
        if self.status.as_deref().is_some_and(|v| {
            !matches!(
                v,
                "accepted"
                    | "unavailable"
                    | "invalid_response"
                    | "limit_exceeded"
                    | "cancelled"
                    | "timed_out"
            ) && !v
                .parse::<u16>()
                .is_ok_and(|code| (100..=599).contains(&code))
        }) {
            return Err("Unknown terminal status");
        }
        self.cursor()?;
        Ok(())
    }
    fn cursor(&self) -> Result<Option<(u64, Uuid)>, &'static str> {
        let Some(cursor) = &self.cursor else {
            return Ok(None);
        };
        if cursor.len() > 57 {
            return Err("Invalid usage cursor");
        }
        let (ts, id) = cursor.split_once(':').ok_or("Invalid usage cursor")?;
        Ok(Some((
            ts.parse().map_err(|_| "Invalid usage cursor")?,
            id.parse().map_err(|_| "Invalid usage cursor")?,
        )))
    }
    fn matches(&self, row: &Event) -> bool {
        matches!(
            (self.source, row.source),
            (SourceFilter::All, _)
                | (SourceFilter::Session, Source::Session)
                | (SourceFilter::Relay, Source::Relay)
        ) && self
            .model
            .as_deref()
            .is_none_or(|model| Some(model) == row.model.as_deref())
            && self.status.as_deref().is_none_or(|status| {
                status == row.outcome.as_str()
                    || status
                        .parse::<u16>()
                        .ok()
                        .is_some_and(|code| row.status_code == Some(code))
            })
            && self.q.as_ref().is_none_or(|q| {
                let q = q.to_lowercase();
                row.event_id.to_string().contains(&q)
                    || [
                        Some(row.source_id.as_str()),
                        row.request_id.as_deref(),
                        row.source_label.as_deref(),
                    ]
                    .into_iter()
                    .flatten()
                    .any(|value| value.to_lowercase().contains(&q))
            })
    }
}

#[derive(Default, Serialize)]
struct TokenTotal {
    reported_total: Option<u64>,
    reported_samples: u64,
    unreported_samples: u64,
    overflow: bool,
}
impl TokenTotal {
    fn add(&mut self, value: Option<u64>, samples: u64) {
        if let Some(value) = value {
            self.reported_samples = self.reported_samples.saturating_add(samples);
            if !self.overflow {
                self.reported_total = self.reported_total.unwrap_or(0).checked_add(value);
                self.overflow = self.reported_total.is_none();
            }
        } else {
            self.unreported_samples = self.unreported_samples.saturating_add(samples);
        }
    }
}

#[derive(Default)]
struct Aggregate {
    requests: usize,
    usage: BTreeMap<&'static str, TokenTotal>,
    attempted: u64,
    paired: u64,
    cache_input: TokenTotal,
    cache_read: TokenTotal,
    latency: (u128, usize),
    ttft: (u128, usize),
    cost: f64,
    saved: f64,
    priced: usize,
    savings_covered: usize,
    rough: usize,
}
fn tokens(usage: Option<&Counters>) -> [(&'static str, Option<u64>); 8] {
    [
        ("input_tokens", usage.and_then(|u| u.input_tokens)),
        ("output_tokens", usage.and_then(|u| u.output_tokens)),
        ("cache_read_tokens", usage.and_then(|u| u.cache_read_tokens)),
        (
            "cache_write_tokens",
            usage.and_then(|u| u.cache_write_tokens),
        ),
        ("reasoning_tokens", usage.and_then(|u| u.reasoning_tokens)),
        (
            "uncached_input_tokens",
            usage.and_then(|u| u.uncached_input_tokens),
        ),
        (
            "non_reasoning_output_tokens",
            usage.and_then(|u| u.non_reasoning_output_tokens),
        ),
        ("total_tokens", usage.and_then(|u| u.total_tokens)),
    ]
}
impl Aggregate {
    fn add(&mut self, row: &Event, price: &Price) {
        self.requests += 1;
        let samples = row.usage.as_ref().map_or(1, |u| u.samples);
        self.attempted = self.attempted.saturating_add(samples);
        for (name, value) in tokens(row.usage.as_ref()) {
            self.usage.entry(name).or_default().add(value, samples);
        }
        if let Some((input, cache)) = pair(row.usage.as_ref()) {
            self.paired = self.paired.saturating_add(samples);
            self.cache_input.add(Some(input), samples);
            self.cache_read.add(Some(cache), samples);
        }
        for (metric, value) in [
            (&mut self.latency, row.latency_ms),
            (&mut self.ttft, row.ttft_ms),
        ] {
            if let Some(ms) = value {
                metric.0 += u128::from(ms);
                metric.1 += 1;
            }
        }
        if let Some(cost) = price.estimated_cost_usd {
            self.cost += cost;
            self.priced += 1;
        }
        if let Some(saved) = price.estimated_saved_usd {
            self.saved += saved;
            self.savings_covered += 1;
        }
        if price.rough_input_estimate {
            self.rough += 1;
        }
    }
    fn cache(&self) -> ReportedCacheUsage {
        let pair = self
            .cache_input
            .reported_total
            .zip(self.cache_read.reported_total);
        ReportedCacheUsage {
            scope: "reported_samples",
            attempted_samples: self.attempted,
            paired_samples: self.paired,
            input_tokens: pair.map(|(input, _)| input),
            cache_read_tokens: pair.map(|(_, read)| read),
            cache_hit_rate: hit_rate(pair),
        }
    }
    fn json(mut self) -> Value {
        for (name, _) in tokens(None) {
            self.usage.entry(name).or_default();
        }
        let avg = |(total, count): (u128, usize)| (count != 0).then(|| total as f64 / count as f64);
        let cache = self.cache();
        json!({
            "requests": self.requests, "usage": self.usage, "reported_cache": cache,
            "average_latency_ms": avg(self.latency), "latency_samples": self.latency.1,
            "average_ttft_ms": avg(self.ttft), "ttft_samples": self.ttft.1,
            "estimated_cost_usd": (self.priced > 0 && self.cost.is_finite()).then_some(self.cost),
            "estimated_saved_usd": (self.savings_covered > 0 && self.saved.is_finite()).then_some(self.saved),
            "priced_requests": self.priced, "unpriced_requests": self.requests - self.priced,
            "pricing_coverage": (self.requests > 0).then(|| self.priced as f64 / self.requests as f64),
            "rough_priced_requests": self.rough, "savings_covered_requests": self.savings_covered,
        })
    }
}

fn pair(usage: Option<&Counters>) -> Option<(u64, u64)> {
    let usage = usage?;
    usage
        .input_tokens
        .zip(usage.cache_read_tokens)
        .filter(|(input, read)| read <= input)
}
fn hit_rate(pair: Option<(u64, u64)>) -> Option<f64> {
    pair.filter(|(input, _)| *input > 0)
        .map(|(input, read)| read as f64 / input as f64)
}

#[derive(Default, Serialize)]
struct Price {
    estimated_cost_usd: Option<f64>,
    estimated_saved_usd: Option<f64>,
    rough_input_estimate: bool,
}
/// This deliberately does not use the general catalog's approximate lookup.
fn exact_price<'a>(
    catalog: &'a MetadataSnapshot,
    provider: &str,
    model: &str,
) -> Option<&'a ModelPricing> {
    fn unique<'a>(mut rows: impl Iterator<Item = &'a ModelMetadata>) -> Option<&'a ModelPricing> {
        let row = rows.next()?;
        rows.next()
            .is_none()
            .then_some(row.pricing.as_ref())
            .flatten()
    }
    if model.contains('/') {
        let mut full = catalog
            .models
            .iter()
            .filter(|row| row.id == model || row.canonical_slug.as_deref() == Some(model))
            .peekable();
        if full.peek().is_some() {
            return unique(full);
        }
    }
    let mut scoped = catalog
        .models
        .iter()
        .filter(|row| row.provider == provider && row.model_id == model)
        .peekable();
    if scoped.peek().is_some() {
        return unique(scoped);
    }
    unique(catalog.models.iter().filter(|row| row.model_id == model))
}
fn estimate(price: Option<&ModelPricing>, usage: Option<&Counters>) -> Price {
    let Some((price, usage)) = price.zip(usage) else {
        return Price::default();
    };
    let rate = |value: &Option<String>| {
        value
            .as_deref()?
            .parse::<f64>()
            .ok()
            .filter(|r| r.is_finite() && *r >= 0.0)
    };
    let prompt = rate(&price.prompt);
    let completion = rate(&price.completion);
    let read_rate = rate(&price.input_cache_read);
    let charge = |tokens: u64, rate: Option<f64>| {
        if tokens == 0 {
            Some(0.0)
        } else {
            rate.map(|rate| tokens as f64 * rate)
        }
    };
    let saved = pair(Some(usage))
        .and_then(|(_, read)| Some(read as f64 * (prompt? - read_rate?).max(0.0)))
        .filter(|v| v.is_finite());
    let components = |input: u64| {
        let read = usage.cache_read_tokens?;
        let write = usage.cache_write_tokens?;
        let uncached = input.checked_sub(read)?.checked_sub(write)?;
        Some((uncached, read, write))
    };
    let mut rough = false;
    let cost = (|| {
        let input = usage.input_tokens?;
        // A catalog row without both public base rates is not a priced request.
        // An explicit zero rate is valid; a missing rate is not a zero rate.
        let output_cost = charge(usage.output_tokens?, Some(completion?))?;
        let prompt = Some(prompt?);
        let input_cost = if let Some((uncached, read, write)) = components(input) {
            charge(uncached, prompt)?
                + charge(read, read_rate)?
                + charge(write, rate(&price.input_cache_write))?
        } else {
            rough = true;
            charge(input, prompt)?
        };
        let total = input_cost + output_cost;
        total.is_finite().then_some(total)
    })();
    Price {
        estimated_cost_usd: cost,
        estimated_saved_usd: saved,
        rough_input_estimate: cost.is_some() && rough,
    }
}

fn coverage(snapshot: &Snapshot, window: &Window) -> Value {
    json!({"state":"observed_only", "backfilled":false, "completeness":"unknown", "activated_at_ms":snapshot.observed_since_ms,
        "retention_cutoff_ms":window.ends_at_ms.saturating_sub(RETENTION_MS), "capacity":CAPACITY, "capacity_reached":snapshot.capacity_reached,
        "oldest_retained_event_ms": snapshot.rows.last().map(|r|r.ts_ms),
        "pricing_source":"openrouter_public_token_rates"})
}

fn event_price<'a>(catalog: &'a MetadataSnapshot, row: &Event) -> Option<&'a ModelPricing> {
    exact_price(catalog, row.provider_id.as_deref()?, row.model.as_deref()?)
}

impl super::super::Manager {
    pub(crate) async fn dashboard_summary(
        &self,
        owner: &str,
        filter: &Filter,
        window: &Window,
    ) -> Result<Value, Error> {
        let snapshot = self.dashboard.snapshot(owner).await?;
        let catalog = self.model_metadata.snapshot();
        let mut result = summary(&snapshot, &catalog, filter, window);
        let sessions = self.directory.list_entries(owner).await?;
        let mut active = 0;
        let mut unavailable = 0;
        for session in &sessions {
            match session {
                microcrab_storage::directory::SessionEntry::Available { record } => {
                    active += usize::from(!record.archived)
                }
                microcrab_storage::directory::SessionEntry::Unavailable { .. } => unavailable += 1,
            }
        }
        result["kpi"]["total_sessions"] = json!(sessions.len());
        result["kpi"]["active_sessions"] = json!((unavailable == 0).then_some(active));
        result["kpi"]["unavailable_sessions"] = json!(unavailable);
        let keys = self.relay_keys.list(owner).await.ok();
        result["kpi"]["total_api_keys"] = json!(keys.as_ref().map(Vec::len));
        result["kpi"]["active_api_keys"] = json!(keys.as_ref().map(|keys| {
            keys.iter()
                .filter(|key| key.enabled && !key.revoked)
                .count()
        }));
        Ok(result)
    }

    pub(crate) async fn dashboard_diagnostics(
        &self,
        owner: &str,
        filter: &Filter,
        window: &Window,
    ) -> Result<Value, Error> {
        let snapshot = self.dashboard.snapshot(owner).await?;
        let catalog = self.model_metadata.snapshot();
        let Groups { models, .. } = aggregate(&snapshot, &catalog, filter, window);
        let mut diagnostics = Vec::new();
        for ((provider, model), sample) in &models {
            let cache = sample.cache();
            if cache.paired_samples < cache.attempted_samples {
                diagnostics.push(json!({"code":"cache_usage_unreported","provider_id":provider,"model":model,"paired_samples":cache.paired_samples,"attempted_samples":cache.attempted_samples}));
            }
            if let Some((threshold, hit_rate)) =
                filter.low_cache_hit_warn_pct.zip(cache.cache_hit_rate)
                && hit_rate * 100.0 < f64::from(threshold)
            {
                diagnostics.push(json!({"code":"cache_hit_rate_below_threshold","provider_id":provider,"model":model,"threshold_pct":threshold,"hit_rate":hit_rate,"paired_samples":cache.paired_samples}));
            }
        }
        Ok(
            json!({"models":model_rows(models), "diagnostics":diagnostics, "coverage":coverage(&snapshot,window), "window":window}),
        )
    }

    pub(crate) async fn dashboard_logs(
        &self,
        owner: &str,
        filter: &Filter,
        window: &Window,
    ) -> Result<Value, Error> {
        let snapshot = self.dashboard.snapshot(owner).await?;
        let catalog = self.model_metadata.snapshot();
        let cursor = filter.cursor().map_err(Error::protocol)?;
        let limit = filter.limit.unwrap_or(50);
        let mut rows = snapshot.rows.iter().filter(|row| {
            window.contains(row.ts_ms)
                && filter.matches(row)
                && cursor.is_none_or(|key| row.key() < key)
        });
        let items: Vec<_> = rows
            .by_ref()
            .take(limit)
            .map(|row| log_row(&catalog, row))
            .collect::<Result<_, _>>()?;
        let next_cursor = if rows.next().is_some() {
            items.last().and_then(|item| {
                Some(format!(
                    "{}:{}",
                    item["ts_ms"].as_u64()?,
                    item["event_id"].as_str()?
                ))
            })
        } else {
            None
        };
        Ok(json!({"items":items,"next_cursor":next_cursor,"coverage":coverage(&snapshot,window)}))
    }

    pub(crate) async fn dashboard_csv(
        &self,
        owner: &str,
        filter: &Filter,
        window: &Window,
    ) -> Result<(String, bool), Error> {
        let snapshot = self.dashboard.snapshot(owner).await?;
        let catalog = self.model_metadata.snapshot();
        let mut rows = snapshot
            .rows
            .iter()
            .filter(|row| window.contains(row.ts_ms) && filter.matches(row));
        let csv = export_csv(rows.by_ref().take(CSV_LIMIT), &catalog);
        Ok((csv, rows.next().is_some()))
    }
}

#[derive(Default)]
struct Groups {
    total: Aggregate,
    series: Vec<Aggregate>,
    protocols: BTreeMap<Option<&'static str>, Aggregate>,
    models: BTreeMap<(Option<String>, Option<String>), Aggregate>,
}

fn aggregate(
    snapshot: &Snapshot,
    catalog: &MetadataSnapshot,
    filter: &Filter,
    window: &Window,
) -> Groups {
    let mut groups = Groups {
        series: (0..window.bucket_starts_ms.len())
            .map(|_| Aggregate::default())
            .collect(),
        ..Groups::default()
    };
    let mut rates = BTreeMap::new();
    for row in snapshot
        .rows
        .iter()
        .filter(|row| window.contains(row.ts_ms) && filter.matches(row))
    {
        let route = (row.provider_id.as_ref(), row.model.as_ref());
        let rate = rates
            .entry(route)
            .or_insert_with(|| event_price(catalog, row));
        let price = estimate(*rate, row.usage.as_ref());
        groups.total.add(row, &price);
        let index = window
            .bucket_starts_ms
            .partition_point(|start| *start <= row.ts_ms)
            - 1;
        groups.series[index].add(row, &price);
        groups
            .protocols
            .entry(row.protocol.as_ref().map(|p| p.as_str()))
            .or_default()
            .add(row, &price);
        groups
            .models
            .entry((row.provider_id.clone(), row.model.clone()))
            .or_default()
            .add(row, &price);
    }
    groups
}
fn model_rows(models: BTreeMap<(Option<String>, Option<String>), Aggregate>) -> Vec<Value> {
    models
        .into_iter()
        .map(|((provider, model), bucket)| {
            let mut bucket = bucket.json();
            bucket["provider_id"] = json!(provider);
            bucket["model"] = json!(model);
            bucket
        })
        .collect()
}
fn summary(
    snapshot: &Snapshot,
    catalog: &MetadataSnapshot,
    filter: &Filter,
    window: &Window,
) -> Value {
    let Groups {
        total,
        series,
        protocols,
        models,
    } = aggregate(snapshot, catalog, filter, window);
    let mut kpi = total.json();
    let filtered = snapshot
        .rows
        .iter()
        .filter(|row| row.ts_ms <= window.ends_at_ms && filter.matches(row));
    kpi["today_requests"] = json!(
        filtered
            .clone()
            .filter(|row| row.ts_ms >= window.today_start_ms)
            .count()
    );
    kpi["retained_requests"] = json!(filtered.count());
    let series: Vec<_> = series
        .into_iter()
        .zip(&window.bucket_starts_ms)
        .map(|(bucket, start)| {
            let mut bucket = bucket.json();
            bucket["start_ms"] = json!(start);
            bucket
        })
        .collect();
    let platforms: Vec<_> = protocols
        .into_iter()
        .map(|(protocol, bucket)| {
            let mut bucket = bucket.json();
            bucket["protocol"] = json!(protocol);
            bucket
        })
        .collect();
    json!({"range":window.range,"source":filter.source,"window":window,"coverage":coverage(snapshot,window),"kpi":kpi,"series":series,"platforms":platforms,"models":model_rows(models)})
}

fn log_row(catalog: &MetadataSnapshot, event: &Event) -> Result<Value, Error> {
    let mut value = serde_json::to_value(event)?;
    value["cache_hit_rate"] = json!(hit_rate(pair(event.usage.as_ref())));
    value["pricing"] =
        serde_json::to_value(estimate(event_price(catalog, event), event.usage.as_ref()))?;
    Ok(value)
}

fn csv_cell(value: &str) -> String {
    let unsafe_prefix = value.trim_start().starts_with(['=', '+', '-', '@'])
        || value.starts_with(['\t', '\r', '\n']);
    format!(
        "\"{}{}\"",
        if unsafe_prefix { "'" } else { "" },
        value.replace('"', "\"\"")
    )
}
fn export_csv<'a>(rows: impl Iterator<Item = &'a Event>, catalog: &MetadataSnapshot) -> String {
    let mut csv = String::from(
        "ts_ms,event_id,request_id,source,source_id,source_label,provider_id,model,protocol,outcome,status_code,latency_ms,ttft_ms,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,reasoning_tokens,uncached_input_tokens,non_reasoning_output_tokens,total_tokens,estimated_cost_usd,estimated_saved_usd\r\n",
    );
    for row in rows {
        let price = estimate(event_price(catalog, row), row.usage.as_ref());
        let number = |value: Option<u64>| value.map(|n| n.to_string()).unwrap_or_default();
        let mut fields = vec![
            row.ts_ms.to_string(),
            row.event_id.to_string(),
            csv_cell(row.request_id.as_deref().unwrap_or_default()),
            match row.source {
                Source::Session => "session",
                Source::Relay => "relay",
            }
            .to_owned(),
            csv_cell(&row.source_id),
            csv_cell(row.source_label.as_deref().unwrap_or_default()),
            csv_cell(row.provider_id.as_deref().unwrap_or_default()),
            csv_cell(row.model.as_deref().unwrap_or_default()),
            row.protocol
                .as_ref()
                .map(|p| p.as_str())
                .unwrap_or_default()
                .to_owned(),
            row.outcome.as_str().to_owned(),
            number(row.status_code.map(u64::from)),
            number(row.latency_ms),
            number(row.ttft_ms),
        ];
        fields.extend(
            tokens(row.usage.as_ref())
                .into_iter()
                .map(|(_, count)| number(count)),
        );
        fields.extend(
            [price.estimated_cost_usd, price.estimated_saved_usd]
                .into_iter()
                .map(|v| v.map(|v| v.to_string()).unwrap_or_default()),
        );
        csv.push_str(&fields.join(","));
        csv.push_str("\r\n");
    }
    csv
}

#[cfg(test)]
mod tests {
    use super::*;
    use microcrab_protocol::Usage;

    #[test]
    fn dashboard_browser_boundaries_preserve_short_and_long_dates() {
        let starts = [
            100 * DAY_MS,
            101 * DAY_MS,
            102 * DAY_MS,
            103 * DAY_MS - HOUR_MS,
            104 * DAY_MS - HOUR_MS,
            105 * DAY_MS,
            106 * DAY_MS,
        ];
        let serialized = starts.map(|value| value.to_string()).join(",");
        let now = starts[6] + HOUR_MS;
        let window = Window::new(Range::Days7, now, Some(&serialized), Some(starts[6])).unwrap();
        assert_eq!(window.bucket_starts_ms, starts);
        assert_eq!(window.time_basis, "browser");
        assert!(window.contains(starts[0]));
        assert!(!window.contains(starts[0] - 1));
        assert!(Window::new(Range::Days7, now, Some(&serialized), None).is_err());
        assert!(Window::new(Range::Hours24, now, Some(&serialized), Some(starts[6])).is_err());
    }

    #[test]
    fn dashboard_prices_require_unique_exact_ids_and_known_counters() {
        let catalog: MetadataSnapshot = serde_json::from_value(json!({
            "version":1, "source":"https://openrouter.ai/api/v1/models", "fetched_at_ms":1,
            "models":[
                {"id":"openai/known", "canonical_slug":"openai/exact-alias", "pricing":{"prompt":"0.01", "completion":"0.02", "input_cache_read":"0.005", "input_cache_write":"0.03"}},
                {"id":"other/known", "pricing":{"prompt":"0.05", "completion":"0.06"}},
                {"id":"other/single", "name":"Display Name"}
            ]
        })).unwrap();
        assert!(exact_price(&catalog, "gateway", "known").is_none());
        assert!(exact_price(&catalog, "openai", "Known").is_none());
        assert!(exact_price(&catalog, "openai", "known-2026").is_none());
        assert!(exact_price(&catalog, "other", "Display Name").is_none());
        let exact = exact_price(&catalog, "openai", "known").unwrap();
        assert_eq!(
            exact_price(&catalog, "gateway", "openai/exact-alias"),
            Some(exact)
        );
        let mut usage = Counters::from(&Usage {
            input_tokens: Some(10),
            output_tokens: Some(2),
            cache_read_tokens: Some(3),
            cache_write_tokens: Some(1),
            samples: 1,
            ..Usage::default()
        });
        let price = estimate(Some(exact), Some(&usage));
        assert!((price.estimated_cost_usd.unwrap() - 0.145).abs() < 1e-10);
        assert!((price.estimated_saved_usd.unwrap() - 0.015).abs() < 1e-10);
        assert!(!price.rough_input_estimate);
        usage.cache_write_tokens = None;
        let price = estimate(Some(exact), Some(&usage));
        assert!(price.rough_input_estimate);
        assert!((price.estimated_cost_usd.unwrap() - 0.14).abs() < 1e-10);
        usage.output_tokens = None;
        assert_eq!(estimate(Some(exact), Some(&usage)).estimated_cost_usd, None);
    }
}
