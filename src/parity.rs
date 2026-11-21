//! `parity_openapi`: the computed `OpenAPI` diff between a ported target and its origins.
//!
//! This runs inside the job Pod, on the trees the runner materialised — nothing is fetched, no
//! command is executed, and the only file read is `<source>/openapi.json`. The documents are
//! untrusted input, so every step is bound: document size, `$ref` depth, route count and the
//! report that travels back in the result line.
//!
//! The contract comes from conveyor's rewrite processes (measured 2026-11-19):
//!
//! * `rewrite_slice` — every route the slice names is present in the target with the same method,
//!   the same path and an equal request and response shape; a named route that is missing, renamed
//!   or differing in one field fails, and so does a route the slice did not name appearing in the
//!   target.
//! * `rewrite_parity_qa` — the comparison is against the **union** of every source's document, and
//!   its computed difference set must be empty. The report states only what was computed: no
//!   "acceptable", no "known difference", no dropped comparison.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Instant;

use serde::Serialize;
use serde_json::{Map, Value, json};
use tokio::time::Instant as TokioInstant;

use crate::domain::{JobInput, ParitySpec};
use crate::runner::exec::Cancellation;
use crate::runner::result::{Check, CheckStatus, RunnerResult, Status};

/// The document every source of a parity job carries at its root. The two live fixtures
/// (`conveyor-coding/py2rs-fixture-svc` and its Rust port) both follow it; a per-source override
/// is an additive field later, not a guess now.
pub const DOCUMENT_NAME: &str = "openapi.json";

/// The largest document one source may present. Four times the largest spec in the estate, so a
/// real service never meets it and a hostile one cannot make the runner hold more.
const MAX_DOCUMENT_BYTES: u64 = 16 * 1024 * 1024;

/// How much of a `$ref` chain one schema may unwind. Recursive schemas are normal; the bound turns
/// a cycle into a computed fact (`$recursive`) rather than a hang.
const MAX_REF_DEPTH: usize = 32;

/// How many nodes one comparison may expand through `$ref`s, shared by every route and source.
///
/// `MAX_REF_DEPTH` bounds a chain, not a tree: two references to the same schema at every level of
/// a 31-level document expand to 2^31 nodes, which is a CPU and memory bomb inside a 2 GiB Pod.
/// The budget bounds total work instead. 2 M nodes is far above the largest projected operation in
/// the estate and far below what a diamond needs, so a real document never meets it and a hostile
/// one stops with a named `unreadable` route rather than an OOM.
const MAX_CANONICAL_NODES: usize = 2_000_000;

/// How many differences one route reports. Past this the verdict is already "differs"; the report
/// only has to carry enough to act on.
const MAX_DIFFERENCES_PER_ROUTE: usize = 64;

/// The most target-only routes a report lists. The verdict never depends on a listed extra.
const MAX_EXTRA_ROUTES: usize = 256;

/// The whole report stays inside the result-line budget (`RESULT_LINE_BUDGET` is 192 KiB, and the
/// failing checks may carry 32 KiB tails of their own); `bounded_line` can only drop
/// `checks[].output_tail`, so the cap lives here, at construction.
const MAX_REPORT_BYTES: usize = 96 * 1024;

/// One value quoted in a difference is capped here: the pointer is the actionable part, and a
/// whole schema repeated twice per difference is what would blow the report.
const MAX_VALUE_TEXT: usize = 512;

/// The operation keys an `OpenAPI` path item may declare.
const METHODS: [&str; 8] = [
    "get", "put", "post", "delete", "options", "head", "patch", "trace",
];

/// Keys that describe a document rather than a wire shape. Everything else is compared verbatim.
const ANNOTATIVE: [&str; 12] = [
    "description",
    "summary",
    "title",
    "example",
    "examples",
    "externalDocs",
    "operationId",
    "tags",
    "deprecated",
    "servers",
    "security",
    "callbacks",
];

/// Which of the two contracts this job asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// The slice's own routes, and nothing else in the target.
    Slice,
    /// Every route the origins' union declares.
    Union,
}

/// One route as the report names it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct RouteRef {
    /// Uppercase HTTP method.
    pub method: String,
    /// The `OpenAPI` path, verbatim (`/api/jobs/{id}`).
    pub path: String,
}

/// What the comparison found for one route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteStatus {
    /// Present in the target and equal to every origin that declares it.
    Equal,
    /// Present, but at least one difference was computed.
    Differ,
    /// The target does not declare the route.
    Missing,
    /// No origin declares the route the slice named, so there is nothing to compare against.
    NotDeclared,
    /// The target declares a route the slice did not name.
    NotNamed,
    /// A document could not be projected (an external `$ref`, a malformed node).
    Unreadable,
}

/// One computed difference. `expected` is the origin's value, `actual` the target's; `None` means
/// the key is absent on that side. Both are capped — the pointer is the actionable part.
#[derive(Debug, Clone, Serialize)]
pub struct Difference {
    /// The origin this difference was computed against.
    pub source: String,
    /// JSON Pointer inside the route's projection.
    pub pointer: String,
    /// The origin's value, or `None` when the key is absent there.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<Value>,
    /// The target's value, or `None` when the key is absent there.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<Value>,
}

/// The computed state of one route.
#[derive(Debug, Clone, Serialize)]
pub struct RouteReport {
    /// Uppercase HTTP method.
    pub method: String,
    /// The `OpenAPI` path, verbatim.
    pub path: String,
    /// What was found.
    pub status: RouteStatus,
    /// The origins this route was compared with.
    pub against: Vec<String>,
    /// Why the route could not be projected, when it could not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Every computed difference, in a deterministic order.
    pub differences: Vec<Difference>,
}

/// The counts a caller reports without reading every route.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Counts {
    /// Routes in the compared set.
    pub routes: usize,
    /// Routes equal against every origin that declares them.
    pub equal: usize,
    /// Routes with at least one difference (or a projection error).
    pub differ: usize,
    /// Routes the target does not declare.
    pub missing: usize,
    /// Slice mode: routes the target declares that the slice did not name. Union mode: target-only
    /// routes, reported but not failing (the union criterion is one-directional).
    pub extra: usize,
}

/// The structured result of one parity job.
#[derive(Debug, Clone, Serialize)]
pub struct Report {
    /// Which contract ran.
    pub mode: Mode,
    /// The source that carries the rewrite's contract.
    pub target: String,
    /// Every source the job named, in order.
    pub sources: Vec<String>,
    /// The counts above.
    pub counts: Counts,
    /// One entry per route in the compared set.
    pub routes: Vec<RouteReport>,
    /// Routes declared only by the target.
    pub extra: Vec<RouteRef>,
    /// Sources whose document could not be loaded, with the reason.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
    /// Whether the cap dropped entries; counts always describe the whole comparison.
    pub truncated: bool,
}

impl Report {
    /// Keep the report inside [`MAX_REPORT_BYTES`], dropping the least important entries first:
    /// differences, then whole route entries, then extra routes. The counts and the verdict are
    /// computed from the whole comparison before any of this, so truncation never changes them.
    /// The last-resort trim for the result writer: keep the counts the verdict came from, drop the
    /// per-route detail. Used only when even a capped report would not fit the result line.
    pub fn drop_entries(&mut self) {
        self.truncated = true;
        self.routes.clear();
        self.extra.clear();
    }

    fn capped(mut self) -> Self {
        if serialised_len(&self) <= MAX_REPORT_BYTES {
            return self;
        }
        self.truncated = true;
        // Coarse steps, not one entry per serialisation: a 16,000-difference report must not cost
        // 16,000 serialisations. Two differences per route keep the report actionable; the route
        // list then halves until it fits; the counts survive all of it.
        for route in &mut self.routes {
            route.differences.truncate(2);
        }
        self.extra.truncate(100);
        let mut keep = self.routes.len().max(1);
        while serialised_len(&self) > MAX_REPORT_BYTES {
            keep /= 2;
            if keep == 0 {
                break;
            }
            self.routes.truncate(keep);
        }
        if serialised_len(&self) > MAX_REPORT_BYTES {
            for route in &mut self.routes {
                route.differences.clear();
            }
        }
        if serialised_len(&self) > MAX_REPORT_BYTES {
            self.drop_entries();
        }
        self
    }
}

fn serialised_len(report: &Report) -> usize {
    serde_json::to_vec(report).map_or(usize::MAX, |bytes| bytes.len())
}

/// One source's document, after loading and operation extraction.
struct Loaded {
    name: String,
    document: Value,
    operations: BTreeMap<RouteRef, Operation>,
}

/// One operation, owned: the documents are freed after extraction, and a projection needs the
/// operation's own object plus the path-item parameters that apply to it.
#[derive(Debug, Clone)]
struct Operation {
    operation: Value,
    path_parameters: Value,
}

/// Compare a job's sources and return the result. Runs inside the Pod; always returns a result.
// One linear sequence — load, compare, emit — where every branch must still produce the result
// line; splitting it into helpers would scatter that guarantee across call sites.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    input: &JobInput,
    sources: &[(String, PathBuf)],
    deadline: TokioInstant,
    cancel: &mut Cancellation,
    mut result: RunnerResult,
) -> RunnerResult {
    let Some(spec) = input.parity.clone() else {
        result.status = Status::InvalidSpec;
        result.summary = String::from("kind parity_openapi carries no input.parity");
        result.error = Some("the job spec has no parity contract to compare".to_owned());
        return result;
    };

    let names: Vec<String> = sources.iter().map(|(name, _)| name.clone()).collect();
    let mut load_errors: Vec<String> = Vec::new();
    let mut loaded: Vec<Loaded> = Vec::new();
    let started = Instant::now();
    for (name, dir) in sources {
        if cancel.received().await {
            return stop(
                result,
                Status::Cancelled,
                "the job was cancelled during parity",
            );
        }
        if TokioInstant::now() >= deadline {
            return stop(
                result,
                Status::Timeout,
                "the job deadline was reached during parity",
            );
        }
        match load(dir, name) {
            Ok(source) => loaded.push(source),
            Err(reason) => load_errors.push(format!("source {name:?}: {reason}")),
        }
    }

    if !load_errors.is_empty() {
        // A document that is missing or unreadable is a fact about the source, never
        // infrastructure: the caller must see a failed check naming it, not exit 3.
        result.checks.push(Check {
            source: None,
            name: "openapi documents".to_owned(),
            status: CheckStatus::Failed,
            exit_code: Some(1),
            duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            output_tail: Some(load_errors.join("\n")),
        });
        result.parity = Some(
            Report {
                mode: mode_of(&spec),
                target: spec.target.clone(),
                sources: names,
                counts: Counts::default(),
                routes: Vec::new(),
                extra: Vec::new(),
                errors: load_errors,
                truncated: false,
            }
            .capped(),
        );
        let finished = finish(result);
        return RunnerResult {
            summary: "the comparison did not run: a source document could not be read".to_owned(),
            ..finished
        };
    }

    // Checked between documents and again before the verdict. The comparison itself is purely
    // in-memory and bounded (≤500 routes, ≤16 MiB documents, a capped report), so it finishes in
    // seconds — a cancel that lands mid-comparison is seen here, before anything is written.
    if cancel.received().await {
        return stop(
            result,
            Status::Cancelled,
            "the job was cancelled during parity",
        );
    }
    if TokioInstant::now() >= deadline {
        return stop(
            result,
            Status::Timeout,
            "the job deadline was reached during parity",
        );
    }

    let comparison = compare(&spec, &loaded);
    if cancel.received().await {
        return stop(
            result,
            Status::Cancelled,
            "the job was cancelled during parity",
        );
    }
    if TokioInstant::now() >= deadline {
        return stop(
            result,
            Status::Timeout,
            "the job deadline was reached during parity",
        );
    }
    let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    result.checks.push(Check {
        source: None,
        name: "openapi documents".to_owned(),
        status: CheckStatus::Passed,
        exit_code: Some(0),
        duration_ms: elapsed,
        output_tail: None,
    });
    result.checks.push(Check {
        source: None,
        name: "openapi diff".to_owned(),
        status: if comparison.passed() {
            CheckStatus::Passed
        } else {
            CheckStatus::Failed
        },
        exit_code: Some(i32::from(!comparison.passed())),
        duration_ms: elapsed,
        output_tail: (!comparison.passed()).then(|| comparison.summary_lines()),
    });
    if comparison.report.mode == Mode::Slice {
        let extras = comparison.report.extra.len();
        result.checks.push(Check {
            source: None,
            name: "openapi routes not named".to_owned(),
            status: if extras == 0 {
                CheckStatus::Passed
            } else {
                CheckStatus::Failed
            },
            exit_code: Some(i32::from(extras != 0)),
            duration_ms: 0,
            output_tail: (extras != 0).then(|| {
                comparison
                    .report
                    .extra
                    .iter()
                    .map(|route| format!("{} {}", route.method, route.path))
                    .collect::<Vec<_>>()
                    .join("\n")
            }),
        });
    }

    let summary = comparison.summary();
    result.parity = Some(comparison.report.capped());
    let finished = finish(result);
    RunnerResult {
        summary,
        ..finished
    }
}

fn stop(mut result: RunnerResult, status: Status, summary: &str) -> RunnerResult {
    result.status = status;
    result.summary = String::from(summary);
    result
}

/// Derive the verdict from the checks (the shared rule) — then the caller replaces the summary
/// with the parity one, which carries the counts a reader needs.
fn finish(mut result: RunnerResult) -> RunnerResult {
    let total = result.checks.len();
    let failing = result
        .checks
        .iter()
        .filter(|check| check.status == CheckStatus::Failed)
        .count();
    if failing == 0 {
        result.status = Status::Succeeded;
    } else {
        result.status = Status::Failed;
        result.summary = format!("{failing} of {total} checks failed");
    }
    result
}

fn mode_of(spec: &ParitySpec) -> Mode {
    if spec.routes.is_empty() {
        Mode::Union
    } else {
        Mode::Slice
    }
}

/// The outcome of a comparison: the report, the verdict, and the lines a failing check quotes.
struct Comparison {
    report: Report,
    failed: bool,
}

impl Comparison {
    fn passed(&self) -> bool {
        !self.failed
    }

    fn summary(&self) -> String {
        let counts = self.report.counts;
        match self.report.mode {
            Mode::Slice => format!(
                "{} of {} slice routes equal, {} differ, {} missing, {} routes not named",
                counts.equal, counts.routes, counts.differ, counts.missing, counts.extra
            ),
            Mode::Union => format!(
                "{} of {} union routes equal, {} differ, {} missing, {} target-only routes",
                counts.equal, counts.routes, counts.differ, counts.missing, counts.extra
            ),
        }
    }

    /// The first differences, one per line — enough for the check's `output_tail`; the full set is
    /// in the structured report.
    fn summary_lines(&self) -> String {
        let mut lines = Vec::new();
        for route in &self.report.routes {
            if route.status == RouteStatus::Equal {
                continue;
            }
            if let Some(error) = &route.error {
                lines.push(format!("{} {}: {error}", route.method, route.path));
                continue;
            }
            for difference in &route.differences {
                lines.push(format!(
                    "{} {} [{}] {}: expected {}, got {}",
                    route.method,
                    route.path,
                    difference.source,
                    difference.pointer,
                    quote(difference.expected.as_ref()),
                    quote(difference.actual.as_ref()),
                ));
                if lines.len() >= 20 {
                    break;
                }
            }
            if lines.len() >= 20 {
                break;
            }
        }
        if lines.is_empty() {
            "no differences".to_owned()
        } else {
            lines.join("\n")
        }
    }
}

fn quote(value: Option<&Value>) -> String {
    match value {
        Some(value) => {
            let text = value.to_string();
            if text.len() <= 120 {
                text
            } else {
                let mut cut = 120;
                while !text.is_char_boundary(cut) {
                    cut -= 1;
                }
                format!("{}…", &text[..cut])
            }
        }
        None => "<absent>".to_owned(),
    }
}

fn failed(report: Report, errors: Vec<String>) -> Comparison {
    Comparison {
        report: Report { errors, ..report },
        failed: true,
    }
}

/// Compare the loaded documents. Pure: no clock, no I/O, so the tests drive it directly.
// reason: one pass over the routes is the contract — the counts, the per-route entries and the
// verdict are built together, and splitting them would invite the three drifting apart.
#[allow(clippy::too_many_lines)]
fn compare(spec: &ParitySpec, loaded: &[Loaded]) -> Comparison {
    compare_with_budget(spec, loaded, MAX_CANONICAL_NODES)
}

/// [`compare`] with an explicit expansion budget. The production constant is generous enough
/// that no test can reach it without allocating what it is meant to prevent, so the budget is a
/// parameter: a test proves the *mechanism* with a small one.
// reason: one linear route loop is where the counts, the failed flag and the truncation flag stay
// consistent; a helper per branch would spread that state across four call sites.
#[allow(clippy::too_many_lines)]
fn compare_with_budget(spec: &ParitySpec, loaded: &[Loaded], node_budget: usize) -> Comparison {
    let mode = mode_of(spec);
    let Some(target) = loaded.iter().find(|source| source.name == spec.target) else {
        return failed(
            Report {
                mode,
                target: spec.target.clone(),
                sources: loaded.iter().map(|source| source.name.clone()).collect(),
                counts: Counts::default(),
                routes: Vec::new(),
                extra: Vec::new(),
                errors: Vec::new(),
                truncated: false,
            },
            vec![format!(
                "input.parity.target {:?} names no source in this job",
                spec.target
            )],
        );
    };
    let origins: Vec<&Loaded> = loaded
        .iter()
        .filter(|source| source.name != spec.target)
        .collect();

    let route_set: BTreeSet<RouteRef> = match mode {
        Mode::Slice => spec
            .routes
            .iter()
            .map(|route| RouteRef {
                method: route.method.as_str().to_owned(),
                path: route.path.clone(),
            })
            .collect(),
        Mode::Union => origins
            .iter()
            .flat_map(|source| source.operations.keys().cloned())
            .collect(),
    };

    let mut counts = Counts::default();
    let mut routes = Vec::new();
    // The strings behind a failure were never read: only whether any route failed. A bool says
    // the same thing without allocating one string per missing, undeclared or differing route.
    let mut failed = false;
    let mut truncated = false;
    // One budget for the whole comparison, not one per route: the bound is on total work.
    let mut budget = node_budget;

    for route in &route_set {
        counts.routes += 1;
        let against: Vec<&Loaded> = origins
            .iter()
            .copied()
            .filter(|source| source.operations.contains_key(route))
            .collect();
        let Some(target_operation) = target.operations.get(route) else {
            counts.missing += 1;
            failed = true;
            routes.push(RouteReport {
                method: route.method.clone(),
                path: route.path.clone(),
                status: RouteStatus::Missing,
                against: against.iter().map(|source| source.name.clone()).collect(),
                error: None,
                differences: Vec::new(),
            });
            continue;
        };
        if against.is_empty() {
            counts.missing += 1;
            failed = true;
            routes.push(RouteReport {
                method: route.method.clone(),
                path: route.path.clone(),
                status: RouteStatus::NotDeclared,
                against: Vec::new(),
                error: None,
                differences: Vec::new(),
            });
            continue;
        }
        let target_projection = project(&target.document, target_operation, &mut budget);
        let mut differences = Vec::new();
        let mut error: Option<String> = None;
        match target_projection {
            Err(reason) => {
                error = Some(format!(
                    "the target's operation cannot be projected: {reason}"
                ));
            }
            Ok(target_projected) => {
                for origin in &against {
                    if differences.len() >= MAX_DIFFERENCES_PER_ROUTE {
                        truncated = true;
                        break;
                    }
                    let Some(operation) = origin.operations.get(route) else {
                        continue;
                    };
                    match project(&origin.document, operation, &mut budget) {
                        Ok(projected) => {
                            diff(
                                &projected,
                                &target_projected,
                                "",
                                &origin.name,
                                &mut differences,
                                MAX_DIFFERENCES_PER_ROUTE,
                            );
                            if differences.len() >= MAX_DIFFERENCES_PER_ROUTE {
                                truncated = true;
                            }
                        }
                        Err(reason) => {
                            error = Some(format!(
                                "source {:?}: the operation cannot be projected: {reason}",
                                origin.name
                            ));
                        }
                    }
                }
            }
        }
        let status = if error.is_some() {
            RouteStatus::Unreadable
        } else if differences.is_empty() {
            RouteStatus::Equal
        } else {
            RouteStatus::Differ
        };
        match status {
            RouteStatus::Equal => counts.equal += 1,
            RouteStatus::Differ | RouteStatus::Unreadable => {
                counts.differ += 1;
                failed = true;
            }
            _ => {}
        }
        routes.push(RouteReport {
            method: route.method.clone(),
            path: route.path.clone(),
            status,
            against: against.iter().map(|source| source.name.clone()).collect(),
            error,
            differences,
        });
    }

    // Routes the target declares outside the compared set. In slice mode these are the seed's
    // "a route the slice did not name appearing in the target" and fail; in union mode the
    // criterion is one-directional, so they are reported and do not judge.
    let mut extra: Vec<RouteRef> = target
        .operations
        .keys()
        .filter(|route| !route_set.contains(*route))
        .cloned()
        .collect();
    counts.extra = extra.len();
    let slice_extras = mode == Mode::Slice && !extra.is_empty();
    if slice_extras {
        // Counted once in `extra`; a named-set route report is not added for it, so the counts
        // stay disjoint (a route cannot be both in the set and extra).
        failed = true;
    }
    if extra.len() > MAX_EXTRA_ROUTES {
        extra.truncate(MAX_EXTRA_ROUTES);
        truncated = true;
    }

    let report = Report {
        mode,
        target: spec.target.clone(),
        sources: loaded.iter().map(|source| source.name.clone()).collect(),
        counts,
        routes,
        extra,
        errors: Vec::new(),
        truncated,
    };
    Comparison { report, failed }
}

/// Load one source's document and extract its operations.
fn load(dir: &std::path::Path, name: &str) -> Result<Loaded, String> {
    let path = dir.join(DOCUMENT_NAME);
    let metadata = std::fs::metadata(&path)
        .map_err(|error| format!("{DOCUMENT_NAME} is not readable: {error}"))?;
    if metadata.len() > MAX_DOCUMENT_BYTES {
        return Err(format!(
            "{DOCUMENT_NAME} is {} bytes, over the {MAX_DOCUMENT_BYTES}-byte cap",
            metadata.len()
        ));
    }
    let bytes = std::fs::read(&path)
        .map_err(|error| format!("{DOCUMENT_NAME} could not be read: {error}"))?;
    let document: Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("{DOCUMENT_NAME} is not JSON: {error}"))?;
    let operations = operations(&document)?;
    Ok(Loaded {
        name: name.to_owned(),
        document,
        operations,
    })
}

/// Every operation the document declares, keyed by (method, path).
fn operations(document: &Value) -> Result<BTreeMap<RouteRef, Operation>, String> {
    let Some(Value::Object(paths)) = document.get("paths") else {
        return Err("the document has no `paths` object".to_owned());
    };
    let mut operations = BTreeMap::new();
    for (path, item) in paths {
        let item = resolve_item(document, item)?;
        let Some(item) = item.as_object() else {
            return Err(format!("paths[{path:?}] is not an object"));
        };
        let path_parameters = item
            .get("parameters")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new()));
        for method in METHODS {
            let Some(operation) = item.get(method) else {
                continue;
            };
            if !operation.is_object() {
                return Err(format!("paths[{path:?}].{method} is not an object"));
            }
            operations.insert(
                RouteRef {
                    method: method.to_uppercase(),
                    path: path.clone(),
                },
                Operation {
                    operation: operation.clone(),
                    path_parameters: path_parameters.clone(),
                },
            );
        }
    }
    Ok(operations)
}

/// A path item may itself be a `$ref`; follow local ones so its methods are seen.
fn resolve_item(document: &Value, item: &Value) -> Result<Value, String> {
    let Some(object) = item.as_object() else {
        return Ok(item.clone());
    };
    let Some(Value::String(pointer)) = object.get("$ref") else {
        return Ok(item.clone());
    };
    let target = resolve_pointer(document, pointer)?;
    Ok(target.clone())
}

/// Follow a local `$ref`. An external reference cannot be resolved from one tree and is a named
/// failure, never a silent pass.
fn resolve_pointer<'a>(document: &'a Value, pointer: &str) -> Result<&'a Value, String> {
    let Some(rest) = pointer.strip_prefix("#/") else {
        return Err(format!(
            "external $ref {pointer:?} cannot be resolved: only local references (#/...) are supported"
        ));
    };
    let mut node = document;
    for raw in rest.split('/') {
        let token = raw.replace("~1", "/").replace("~0", "~");
        node = match node {
            Value::Object(map) => map
                .get(&token)
                .ok_or_else(|| format!("$ref {pointer:?} names the missing node {token:?}"))?,
            Value::Array(items) => {
                let index: usize = token
                    .parse()
                    .map_err(|_| format!("$ref {pointer:?} has a non-numeric index {token:?}"))?;
                items
                    .get(index)
                    .ok_or_else(|| format!("$ref {pointer:?} names the missing index {token:?}"))?
            }
            _ => {
                return Err(format!(
                    "$ref {pointer:?} descends through a non-container at {token:?}"
                ));
            }
        };
    }
    Ok(node)
}

/// Project one operation to the comparable shape: parameters, request body and responses.
///
/// Annotations are dropped here (and in [`canonical`]); everything else is compared verbatim, so
/// "differing in one field" is reported at that field's pointer.
fn project(document: &Value, operation: &Operation, budget: &mut usize) -> Result<Value, String> {
    let mut parameters = Map::new();
    let operation_parameters = operation
        .operation
        .get("parameters")
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    for list in [&operation.path_parameters, &operation_parameters] {
        let Value::Array(items) = list else {
            continue;
        };
        for item in items {
            let canonical = canonical(document, item, &mut Vec::new(), budget)?;
            let Some(object) = canonical.as_object() else {
                return Err("a parameter is not an object".to_owned());
            };
            let location = object.get("in").and_then(Value::as_str).unwrap_or_default();
            let name = object
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            // Operation-level parameters override path-level ones with the same (in, name).
            parameters.insert(format!("{location}:{name}"), canonical.clone());
        }
    }
    let request_body = match operation.operation.get("requestBody") {
        Some(body) => {
            let canonical = canonical(document, body, &mut Vec::new(), budget)?;
            let Some(object) = canonical.as_object() else {
                return Err("requestBody is not an object".to_owned());
            };
            json!({
                "required": object.get("required").cloned().unwrap_or(Value::Bool(false)),
                "content": content(object)?,
            })
        }
        None => Value::Null,
    };
    let mut responses = Map::new();
    if let Some(responses_object) = operation.operation.get("responses") {
        let canonical = canonical(document, responses_object, &mut Vec::new(), budget)?;
        let Some(map) = canonical.as_object() else {
            return Err("responses is not an object".to_owned());
        };
        for (code, response) in map {
            let Some(response) = response.as_object() else {
                return Err(format!("responses[{code:?}] is not an object"));
            };
            responses.insert(
                code.clone(),
                json!({
                    "content": content(response)?,
                    "headers": response.get("headers").cloned().unwrap_or(Value::Null),
                }),
            );
        }
    }
    Ok(json!({
        "parameters": Value::Object(parameters),
        "request_body": request_body,
        "responses": Value::Object(responses),
    }))
}

/// The media-type map of a canonicalised `content` object: each media type to its schema.
///
/// An absent `content` is an empty map (a response with no body is normal); a `content` that is
/// present but malformed is a named projection error, never silently dropped — a dropped body
/// would compare equal to a document that has none.
fn content(object: &Map<String, Value>) -> Result<Value, String> {
    let Some(content) = object.get("content") else {
        return Ok(Value::Object(Map::new()));
    };
    let Some(content) = content.as_object() else {
        return Err("content is not an object".to_owned());
    };
    let mut out = Map::new();
    for (media_type, entry) in content {
        let Some(entry) = entry.as_object() else {
            return Err(format!("content[{media_type:?}] is not an object"));
        };
        out.insert(
            media_type.clone(),
            entry.get("schema").cloned().unwrap_or(Value::Null),
        );
    }
    Ok(Value::Object(out))
}

/// Resolve local `$ref`s and drop annotative keys. A reference already being unwound becomes
/// `{"$recursive": "<pointer>"}`, so a recursive schema compares equal to an identical one.
///
/// `budget` bounds the total number of nodes one comparison may expand. A 31-level diamond of
/// two references per level is 2^31 nodes under plain recursion; the budget turns that into a
/// named error on the route instead of an OOM inside the Pod.
fn canonical(
    document: &Value,
    node: &Value,
    stack: &mut Vec<String>,
    budget: &mut usize,
) -> Result<Value, String> {
    if *budget == 0 {
        return Err(format!(
            "$ref expansion exceeds the {MAX_CANONICAL_NODES}-node comparison budget"
        ));
    }
    *budget -= 1;
    if let Value::Object(map) = node
        && let Some(Value::String(pointer)) = map.get("$ref")
    {
        if stack.iter().any(|seen| seen == pointer) {
            return Ok(json!({"$recursive": pointer}));
        }
        if stack.len() >= MAX_REF_DEPTH {
            return Err(format!(
                "$ref chain is deeper than {MAX_REF_DEPTH} at {pointer:?}"
            ));
        }
        let target = resolve_pointer(document, pointer)?;
        stack.push(pointer.clone());
        let resolved = canonical(document, target, stack, budget);
        stack.pop();
        let resolved = resolved?;
        // OpenAPI 3.1 allows a `$ref` to carry siblings (3.0 ignored them). Dropping them would
        // compare two different schemas as equal, so they are overlaid on the resolved target.
        let mut siblings = map
            .iter()
            .filter(|(key, _)| {
                key.as_str() != "$ref"
                    && !ANNOTATIVE.contains(&key.as_str())
                    && !key.starts_with("x-")
            })
            .peekable();
        if siblings.peek().is_none() {
            return Ok(resolved);
        }
        let Value::Object(mut out) = resolved else {
            // A non-object target cannot carry object siblings; the target is the schema.
            return Ok(resolved);
        };
        for (key, value) in siblings {
            let child = if is_order_free(key) {
                sorted(value)
            } else {
                value.clone()
            };
            out.insert(key.clone(), canonical(document, &child, stack, budget)?);
        }
        return Ok(Value::Object(fold_nullable(out)));
    }
    match node {
        Value::Object(map) => {
            let mut out = Map::new();
            for (key, value) in map {
                if ANNOTATIVE.contains(&key.as_str()) || key.starts_with("x-") {
                    continue;
                }
                let child = if is_order_free(key) {
                    sorted(value)
                } else {
                    value.clone()
                };
                out.insert(key.clone(), canonical(document, &child, stack, budget)?);
            }
            Ok(Value::Object(fold_nullable(out)))
        }
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(canonical(document, item, stack, budget)?);
            }
            Ok(Value::Array(out))
        }
        _ => Ok(node.clone()),
    }
}

/// `required`, `enum` and a union `type` are sets: the same members in another order are the same
/// shape.
fn is_order_free(key: &str) -> bool {
    key == "required" || key == "enum" || key == "type"
}

/// Fold the two ways `OpenAPI` spells "may be null" into the 3.1 union form.
///
/// A 3.0 document writes `nullable: true` or `anyOf: [X, {"type": "null"}]`; a 3.1 document
/// writes `type: [..., "null"]`. Both admit exactly the same instances, so comparing the
/// spellings would report a difference the contract does not ask about. Measured on the live
/// fixtures 2026-11-19: the `FastAPI` origin emits `anyOf` and the hand-written Rust port emits the
/// type union, and without this fold the pair the rewrite flow uses could never compare equal.
fn fold_nullable(mut object: Map<String, Value>) -> Map<String, Value> {
    match object.get("nullable").cloned() {
        // The 3.0 default is `false`; only `true` carries meaning.
        Some(Value::Bool(true)) => {
            object.remove("nullable");
            if let Some(Value::String(current)) = object.get("type").cloned() {
                object.insert("type".to_owned(), sorted(&json!([current, "null"])));
            }
        }
        Some(_) => {
            object.remove("nullable");
        }
        None => {}
    }
    if object.len() != 1 {
        return object;
    }
    let key = if object.contains_key("anyOf") {
        "anyOf"
    } else if object.contains_key("oneOf") {
        "oneOf"
    } else {
        return object;
    };
    let Some(Value::Array(branches)) = object.get(key).cloned() else {
        return object;
    };
    if branches.len() != 2 {
        return object;
    }
    let null_schemas = [json!({"type": "null"}), json!({"type": ["null"]})];
    let other = if null_schemas.contains(&branches[0]) {
        &branches[1]
    } else if null_schemas.contains(&branches[1]) {
        &branches[0]
    } else {
        return object;
    };
    let Value::Object(mut merged) = other.clone() else {
        return object;
    };
    match merged.get("type").cloned() {
        Some(Value::String(kind)) => {
            merged.insert("type".to_owned(), sorted(&json!([kind, "null"])));
        }
        Some(Value::Array(types)) if types.iter().all(Value::is_string) => {
            let mut types = types;
            if !types.iter().any(|value| value == "null") {
                types.push(json!("null"));
            }
            merged.insert("type".to_owned(), sorted(&Value::Array(types)));
        }
        // A branch with no `type` of its own already admits null through its own structure;
        // folding is not needed and would invent a shape.
        _ => return object,
    }
    merged
}

fn sorted(value: &Value) -> Value {
    let Value::Array(items) = value else {
        return value.clone();
    };
    if !items.iter().all(Value::is_string) {
        return value.clone();
    }
    let mut items = items.clone();
    items.sort_by_key(Value::to_string);
    Value::Array(items)
}

/// Compute the differences between an origin's projection and the target's, in a deterministic
/// order. Empty means equal.
fn diff(
    expected: &Value,
    actual: &Value,
    pointer: &str,
    source: &str,
    out: &mut Vec<Difference>,
    limit: usize,
) {
    // The verdict is already "differs" at the limit; more entries only grow the report.
    if out.len() >= limit {
        return;
    }
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) => {
            let keys: BTreeSet<&String> = expected.keys().chain(actual.keys()).collect();
            for key in keys {
                let child = format!("{pointer}/{}", escape(key));
                match (expected.get(key), actual.get(key)) {
                    (Some(expected), Some(actual)) => {
                        diff(expected, actual, &child, source, out, limit);
                    }
                    (Some(expected), None) => out.push(Difference {
                        source: source.to_owned(),
                        pointer: child,
                        expected: Some(bounded(expected)),
                        actual: None,
                    }),
                    (None, Some(actual)) => out.push(Difference {
                        source: source.to_owned(),
                        pointer: child,
                        expected: None,
                        actual: Some(bounded(actual)),
                    }),
                    (None, None) => {}
                }
            }
        }
        (Value::Array(expected), Value::Array(actual)) if expected.len() != actual.len() => {
            out.push(Difference {
                source: source.to_owned(),
                pointer: pointer.to_owned(),
                expected: Some(bounded(&Value::Array(expected.clone()))),
                actual: Some(bounded(&Value::Array(actual.clone()))),
            });
        }
        (Value::Array(expected), Value::Array(actual)) => {
            for (index, (expected, actual)) in expected.iter().zip(actual).enumerate() {
                diff(
                    expected,
                    actual,
                    &format!("{pointer}/{index}"),
                    source,
                    out,
                    limit,
                );
            }
        }
        _ => {
            if expected != actual {
                out.push(Difference {
                    source: source.to_owned(),
                    pointer: pointer.to_owned(),
                    expected: Some(bounded(expected)),
                    actual: Some(bounded(actual)),
                });
            }
        }
    }
}

/// RFC 6901 escaping for one pointer token.
fn escape(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

/// Cap one quoted value; the pointer and the fact of a difference are what matter.
fn bounded(value: &Value) -> Value {
    let text = value.to_string();
    if text.len() <= MAX_VALUE_TEXT {
        return value.clone();
    }
    let mut cut = MAX_VALUE_TEXT;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    Value::String(format!("{}…", &text[..cut]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ParityMethod, ParityRoute};

    fn loaded(name: &str, document: Value) -> Loaded {
        let operations = operations(&document).expect("operations");
        Loaded {
            name: name.to_owned(),
            document,
            operations,
        }
    }

    /// The origin fixture both live repos follow: one route with a parameter, a `$ref`'d schema
    /// and a response body.
    fn origin() -> Value {
        json!({
            "openapi": "3.1.0",
            "paths": {
                "/api/jobs": {
                    "get": {
                        "operationId": "list",
                        "summary": "list jobs",
                        "parameters": [{
                            "in": "query",
                            "name": "limit",
                            "required": false,
                            "schema": {"type": "integer", "format": "int32"}
                        }],
                        "responses": {
                            "200": {
                                "description": "ok",
                                "content": {"application/json": {"schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/Job"}
                                }}}
                            }
                        }
                    }
                }
            },
            "components": {
                "schemas": {
                    "Job": {
                        "type": "object",
                        "required": ["id", "status"],
                        "properties": {
                            "id": {"type": "string"},
                            "status": {"type": "string", "enum": ["queued", "done"]}
                        }
                    }
                }
            }
        })
    }

    fn slice(routes: &[(ParityMethod, &str)], target: &str) -> ParitySpec {
        ParitySpec {
            target: target.to_owned(),
            routes: routes
                .iter()
                .map(|(method, path)| ParityRoute {
                    method: *method,
                    path: (*path).to_owned(),
                })
                .collect(),
        }
    }

    fn one_route() -> ParitySpec {
        slice(&[(ParityMethod::Get, "/api/jobs")], "target")
    }

    #[test]
    fn identical_documents_are_equal() {
        let comparison = compare(
            &one_route(),
            &[loaded("origin", origin()), loaded("target", origin())],
        );
        assert!(comparison.passed(), "{:?}", comparison.report);
        let counts = comparison.report.counts;
        assert_eq!(counts.routes, 1);
        assert_eq!(counts.equal, 1);
    }

    #[test]
    fn annotations_are_not_shape_differences() {
        // The target adds every annotative key there is, in the operation and in the schema.
        let mut target = origin();
        let get = &mut target["paths"]["/api/jobs"]["get"];
        get["description"] = json!("a description the source never had");
        get["tags"] = json!(["jobs"]);
        get["deprecated"] = json!(true);
        get["x-internal"] = json!({"vendor": true});
        target["components"]["schemas"]["Job"]["title"] = json!("Job");
        target["components"]["schemas"]["Job"]["properties"]["id"]["example"] = json!("j-1");
        target["components"]["schemas"]["Job"]["properties"]["id"]["description"] = json!("id");
        let comparison = compare(
            &one_route(),
            &[loaded("origin", origin()), loaded("target", target)],
        );
        assert!(comparison.passed(), "{:?}", comparison.report);
    }

    #[test]
    fn one_field_difference_fails_at_its_pointer() {
        let mut target = origin();
        target["components"]["schemas"]["Job"]["properties"]["id"]["type"] = json!("integer");
        let comparison = compare(
            &one_route(),
            &[loaded("origin", origin()), loaded("target", target)],
        );
        assert!(!comparison.passed());
        let route = &comparison.report.routes[0];
        assert_eq!(route.status, RouteStatus::Differ);
        let difference = &route.differences[0];
        assert_eq!(difference.source, "origin");
        assert!(
            difference.pointer.ends_with("/items/properties/id/type"),
            "{}",
            difference.pointer
        );
        assert_eq!(difference.expected, Some(json!("string")));
        assert_eq!(difference.actual, Some(json!("integer")));
    }

    #[test]
    fn a_missing_named_route_fails_and_names_the_route() {
        let empty = json!({"openapi": "3.1.0", "paths": {}});
        let comparison = compare(
            &one_route(),
            &[loaded("origin", origin()), loaded("target", empty)],
        );
        assert!(!comparison.passed());
        assert_eq!(comparison.report.routes[0].status, RouteStatus::Missing);
        assert_eq!(comparison.report.counts.missing, 1);
    }

    #[test]
    fn a_renamed_path_is_missing_and_also_not_named() {
        // The slice names the source's path; the port declares a different template.
        let renamed = json!({
            "openapi": "3.1.0",
            "paths": {"/api/jobs/{job_id}": {"get": {"responses": {"200": {"description": "ok"}}}}}
        });
        let source = json!({
            "openapi": "3.1.0",
            "paths": {"/api/jobs/{id}": {"get": {"responses": {"200": {"description": "ok"}}}}}
        });
        let comparison = compare(
            &slice(&[(ParityMethod::Get, "/api/jobs/{id}")], "target"),
            &[loaded("origin", source), loaded("target", renamed)],
        );
        assert!(!comparison.passed());
        assert_eq!(comparison.report.routes[0].status, RouteStatus::Missing);
        assert_eq!(comparison.report.counts.extra, 1);
        assert_eq!(comparison.report.extra[0].path, "/api/jobs/{job_id}");
    }

    #[test]
    fn a_route_the_slice_did_not_name_fails_in_slice_mode() {
        let mut target = origin();
        target["paths"]["/api/extra"] =
            json!({"get": {"responses": {"200": {"description": "ok"}}}});
        let comparison = compare(
            &one_route(),
            &[loaded("origin", origin()), loaded("target", target)],
        );
        assert!(!comparison.passed(), "an unnamed target route must fail");
        assert_eq!(comparison.report.counts.extra, 1);
        // The named route itself is still equal: the failure is the unnamed one.
        assert_eq!(comparison.report.routes[0].status, RouteStatus::Equal);
    }

    #[test]
    fn union_mode_requires_every_route_the_origins_declare() {
        let first = origin();
        let mut second = origin();
        second["paths"]["/api/status"] =
            json!({"get": {"responses": {"200": {"description": "ok"}}}});
        // Target serves everything the union declares, plus a route of its own.
        let mut target = origin();
        target["paths"]["/api/status"] =
            json!({"get": {"responses": {"200": {"description": "ok"}}}});
        target["paths"]["/api/own"] = json!({"get": {"responses": {"200": {"description": "ok"}}}});
        let comparison = compare(
            &slice(&[], "target"),
            &[
                loaded("origin-a", first),
                loaded("origin-b", second),
                loaded("target", target),
            ],
        );
        assert!(comparison.passed(), "{:?}", comparison.report);
        assert_eq!(comparison.report.counts.routes, 2);
        assert_eq!(comparison.report.counts.extra, 1, "target-only, not judged");
    }

    #[test]
    fn union_mode_fails_when_a_declared_route_is_missing() {
        let mut second = origin();
        second["paths"]["/api/status"] =
            json!({"get": {"responses": {"200": {"description": "ok"}}}});
        let comparison = compare(
            &slice(&[], "target"),
            &[
                loaded("origin-a", origin()),
                loaded("origin-b", second),
                loaded("target", origin()),
            ],
        );
        assert!(!comparison.passed());
        assert_eq!(comparison.report.counts.missing, 1);
        assert_eq!(comparison.report.counts.equal, 1);
    }

    #[test]
    fn refs_are_resolved_before_comparing() {
        // The origin inlines the schema; the target points at components. Same shape.
        let origin_doc = json!({
            "openapi": "3.1.0",
            "paths": {"/api/jobs": {"get": {"responses": {"200": {"description": "ok",
                "content": {"application/json": {"schema": {
                    "type": "object", "required": ["id"], "properties": {"id": {"type": "string"}}
                }}}}}}}}
        });
        let target_doc = json!({
            "openapi": "3.1.0",
            "paths": {"/api/jobs": {"get": {"responses": {"200": {"description": "ok",
                "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Job"}}}}}}}},
            "components": {"schemas": {"Job": {
                "type": "object", "required": ["id"], "properties": {"id": {"type": "string"}}
            }}}
        });
        let comparison = compare(
            &one_route(),
            &[loaded("origin", origin_doc), loaded("target", target_doc)],
        );
        assert!(comparison.passed(), "{:?}", comparison.report);
    }

    #[test]
    fn a_recursive_schema_terminates_and_compares_equal() {
        let document = json!({
            "openapi": "3.1.0",
            "paths": {"/api/tree": {"get": {"responses": {"200": {"description": "ok",
                "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Node"}}}}}}}},
            "components": {"schemas": {"Node": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "children": {"type": "array", "items": {"$ref": "#/components/schemas/Node"}}
                }
            }}}
        });
        let comparison = compare(
            &slice(&[(ParityMethod::Get, "/api/tree")], "target"),
            &[
                loaded("origin", document.clone()),
                loaded("target", document),
            ],
        );
        assert!(comparison.passed(), "{:?}", comparison.report);
    }

    #[test]
    fn an_external_ref_is_a_named_failure_not_a_pass() {
        let target = json!({
            "openapi": "3.1.0",
            "paths": {"/api/jobs": {"get": {"responses": {"200": {"description": "ok",
                "content": {"application/json": {"schema": {
                    "$ref": "common.json#/components/schemas/Job"
                }}}}}}}}
        });
        let comparison = compare(
            &one_route(),
            &[loaded("origin", origin()), loaded("target", target)],
        );
        assert!(!comparison.passed());
        let route = &comparison.report.routes[0];
        assert_eq!(route.status, RouteStatus::Unreadable);
        assert!(
            route
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("common.json"),
            "{:?}",
            route.error
        );
    }

    #[test]
    fn set_like_lists_compare_without_their_order() {
        let mut target = origin();
        target["components"]["schemas"]["Job"]["required"] = json!(["status", "id"]);
        target["components"]["schemas"]["Job"]["properties"]["status"]["enum"] =
            json!(["done", "queued"]);
        let comparison = compare(
            &one_route(),
            &[loaded("origin", origin()), loaded("target", target)],
        );
        assert!(comparison.passed(), "{:?}", comparison.report);
    }

    #[test]
    fn a_request_body_difference_is_reported_under_request_body() {
        let origin_doc = json!({
            "openapi": "3.1.0",
            "paths": {"/api/jobs": {"post": {
                "requestBody": {"required": false, "content": {"application/json": {"schema": {"type": "object"}}}},
                "responses": {"201": {"description": "created"}}
            }}}
        });
        let mut target_doc = origin_doc.clone();
        target_doc["paths"]["/api/jobs"]["post"]["requestBody"]["required"] = json!(true);
        let comparison = compare(
            &slice(&[(ParityMethod::Post, "/api/jobs")], "target"),
            &[loaded("origin", origin_doc), loaded("target", target_doc)],
        );
        assert!(!comparison.passed());
        let difference = &comparison.report.routes[0].differences[0];
        assert_eq!(difference.pointer, "/request_body/required");
    }

    #[test]
    fn a_document_without_paths_is_refused_by_name() {
        let error = operations(&json!({"openapi": "3.1.0"})).expect_err("no paths");
        assert!(error.contains("paths"), "{error}");
    }

    fn note_documents(origin_schema: &Value, target_schema: &Value) -> (Value, Value) {
        let operation = json!({"post": {
            "requestBody": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/NoteCreate"}}}},
            "responses": {"201": {"description": "created", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/NoteCreate"}}}}}
        }});
        (
            json!({
                "openapi": "3.0.2",
                "paths": {"/api/notes": operation.clone()},
                "components": {"schemas": {"NoteCreate": origin_schema.clone()}}
            }),
            json!({
                "openapi": "3.1.0",
                "paths": {"/api/notes": operation},
                "components": {"schemas": {"NoteCreate": target_schema.clone()}}
            }),
        )
    }

    #[test]
    fn the_two_null_dialects_compare_equal() {
        // The measured fixture pair, in miniature: FastAPI writes `anyOf` + `nullable`, the Rust
        // port writes the 3.1 type union.
        let (origin_doc, target_doc) = note_documents(
            &json!({
                "type": "object",
                "properties": {
                    "tag": {"anyOf": [{"type": "string", "maxLength": 64}, {"type": "null"}], "title": "Tag"},
                    "title": {"type": "string", "nullable": false}
                }
            }),
            &json!({
                "type": "object",
                "properties": {
                    "tag": {"type": ["null", "string"], "maxLength": 64},
                    "title": {"type": "string"}
                }
            }),
        );
        let comparison = compare(
            &slice(&[(ParityMethod::Post, "/api/notes")], "target"),
            &[loaded("origin", origin_doc), loaded("target", target_doc)],
        );
        assert!(comparison.passed(), "{:?}", comparison.report);
    }

    #[test]
    fn a_nullable_difference_is_still_a_difference() {
        // The dialects fold, but the meaning does not: one side allows null, the other does not.
        let (origin_doc, target_doc) = note_documents(
            &json!({
                "type": "object",
                "properties": {"tag": {"anyOf": [{"type": "string"}, {"type": "null"}]}}
            }),
            &json!({
                "type": "object",
                "properties": {"tag": {"type": "string"}}
            }),
        );
        let comparison = compare(
            &slice(&[(ParityMethod::Post, "/api/notes")], "target"),
            &[loaded("origin", origin_doc), loaded("target", target_doc)],
        );
        assert!(!comparison.passed());
        let pointer = &comparison.report.routes[0].differences[0].pointer;
        assert!(pointer.ends_with("/properties/tag/type"), "{pointer}");
    }

    #[test]
    fn a_report_over_the_cap_is_truncated_but_its_counts_survive() {
        let mut report = Report {
            mode: Mode::Union,
            target: "target".to_owned(),
            sources: vec!["target".to_owned(), "origin".to_owned()],
            counts: Counts {
                routes: 2,
                equal: 1,
                differ: 1,
                missing: 0,
                extra: 0,
            },
            routes: Vec::new(),
            extra: Vec::new(),
            errors: Vec::new(),
            truncated: false,
        };
        // 200 routes x 10 differences of two capped values each is far over the cap.
        for index in 0..200 {
            let differences = (0..10)
                .map(|inner| Difference {
                    source: "origin".to_owned(),
                    pointer: format!("/responses/{index}/schema/{inner}"),
                    expected: Some(Value::String("e".repeat(MAX_VALUE_TEXT))),
                    actual: Some(Value::String("a".repeat(MAX_VALUE_TEXT))),
                })
                .collect();
            report.routes.push(RouteReport {
                method: "GET".to_owned(),
                path: format!("/api/{index}"),
                status: RouteStatus::Differ,
                against: vec!["origin".to_owned()],
                error: None,
                differences,
            });
        }
        let report = report.capped();
        assert!(report.truncated);
        assert!(serialised_len(&report) <= MAX_REPORT_BYTES);
        assert_eq!(
            report.counts.routes, 2,
            "counts are not recomputed from the trimmed set"
        );
        assert_eq!(report.counts.differ, 1);

        let mut dropped = report;
        dropped.drop_entries();
        assert!(dropped.routes.is_empty());
        assert_eq!(dropped.counts.routes, 2);
    }

    #[test]
    fn a_ref_diamond_is_unreadable_not_an_oom() {
        // Two references to the next schema at every level. Plain recursion is 2^levels nodes;
        // the budget must stop it and report it, not exhaust the Pod.
        let levels = 12;
        let mut schemas = serde_json::Map::new();
        for level in 0..levels {
            let next = format!("#/components/schemas/S{}", level + 1);
            schemas.insert(
                format!("S{level}"),
                json!({"oneOf": [{"$ref": next.clone()}, {"$ref": next}]}),
            );
        }
        schemas.insert(format!("S{levels}"), json!({"type": "string"}));
        let document = json!({
            "openapi": "3.1.0",
            "paths": {"/api/jobs": {"get": {"responses": {"200": {"description": "ok",
                "content": {"application/json": {"schema": {"$ref": "#/components/schemas/S0"}}}}}}}},
            "components": {"schemas": schemas},
        });

        let comparison = compare_with_budget(
            &one_route(),
            &[loaded("origin", origin()), loaded("target", document)],
            256,
        );
        assert!(!comparison.passed());
        let route = &comparison.report.routes[0];
        assert_eq!(route.status, RouteStatus::Unreadable);
        assert!(
            route
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("comparison budget"),
            "{:?}",
            route.error
        );
    }

    #[test]
    fn a_ref_sibling_is_compared_not_dropped() {
        // OpenAPI 3.1 allows `{"$ref": X, "minimum": 1}`. Dropping the sibling would call two
        // different schemas equal.
        let document = |schema: Value| {
            json!({
                "openapi": "3.1.0",
                "paths": {
                    "/api/jobs": {
                        "get": {
                            "responses": {
                                "200": {
                                    "description": "ok",
                                    "content": {"application/json": {"schema": schema}},
                                }
                            }
                        }
                    }
                },
                "components": {"schemas": {"Limit": {"type": "integer"}}},
            })
        };
        let same = document(json!({"$ref": "#/components/schemas/Limit", "minimum": 1}));
        let other = document(json!({"$ref": "#/components/schemas/Limit", "minimum": 2}));
        let equal = compare(
            &one_route(),
            &[loaded("origin", same.clone()), loaded("target", same)],
        );
        assert!(equal.passed(), "{:?}", equal.report);

        let differ = compare(
            &one_route(),
            &[
                loaded(
                    "origin",
                    document(json!({"$ref": "#/components/schemas/Limit", "minimum": 1})),
                ),
                loaded("target", other),
            ],
        );
        assert!(!differ.passed());
        assert_eq!(differ.report.routes[0].status, RouteStatus::Differ);
        assert!(
            differ.report.routes[0]
                .differences
                .iter()
                .any(|difference| difference.pointer.contains("minimum")),
            "{:?}",
            differ.report.routes[0].differences
        );
    }

    #[test]
    fn a_ref_sibling_annotation_stays_ignored() {
        // `description` is annotative; carrying it beside a `$ref` must not make a document
        // different from the same schema without it.
        let document = |description: Option<&str>| {
            let mut schema = json!({"$ref": "#/components/schemas/Limit"});
            if let Some(description) = description {
                schema["description"] = json!(description);
            }
            json!({
                "openapi": "3.1.0",
                "paths": {
                    "/api/jobs": {
                        "get": {
                            "responses": {
                                "200": {
                                    "description": "ok",
                                    "content": {"application/json": {"schema": schema}},
                                }
                            }
                        }
                    }
                },
                "components": {"schemas": {"Limit": {"type": "integer"}}},
            })
        };
        let comparison = compare(
            &one_route(),
            &[
                loaded("origin", document(None)),
                loaded("target", document(Some("an annotation"))),
            ],
        );
        assert!(comparison.passed(), "{:?}", comparison.report);
    }

    #[test]
    fn differences_are_capped_but_still_differ() {
        let document = |marker: &str| {
            let mut properties = serde_json::Map::new();
            for index in 0..200 {
                properties.insert(
                    format!("p{index:03}"),
                    json!({"type": "string", "pattern": format!("{marker}{index}")}),
                );
            }
            json!({
                "openapi": "3.1.0",
                "paths": {"/api/jobs": {"get": {"responses": {"200": {"description": "ok",
                    "content": {"application/json": {"schema": {
                        "type": "object", "properties": properties
                    }}}}}}}},
            })
        };
        let comparison = compare(
            &one_route(),
            &[
                loaded("origin", document("a")),
                loaded("target", document("b")),
            ],
        );
        assert!(!comparison.passed());
        let route = &comparison.report.routes[0];
        assert_eq!(route.status, RouteStatus::Differ);
        assert_eq!(route.differences.len(), MAX_DIFFERENCES_PER_ROUTE);
        assert!(
            comparison.report.truncated,
            "a report that hit the differences cap says so"
        );
    }
}
