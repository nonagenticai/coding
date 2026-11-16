//! `coding-runner` — the in-pod job executor.
//!
//! It is the ENTRYPOINT of every runner image. It takes **no arguments** in a Pod: the job
//! arrives in the `CODING_JOB_SPEC` environment variable set by [`coding::sandbox`], and the
//! outcome leaves as one NDJSON line on stdout.
//!
//! It never talks to Kubernetes and never starts a container. Kata gives it a VM; the runner
//! only runs the fixed command plan for its kind.

use std::process::ExitCode;

use coding::runner::exec::{self, Cancellation};
use coding::runner::result::{RunnerResult, Status, emit};
use coding::runner::spec::{RunnerSpec, SPEC_ENV};

#[tokio::main]
async fn main() -> ExitCode {
    let mut arguments = std::env::args().skip(1);
    if let Some(first) = arguments.next() {
        match first.as_str() {
            // The image build's smoke test runs this without a cluster or a job.
            "--version" | "-V" => {
                println!("coding-runner {}", env!("CARGO_PKG_VERSION"));
                return ExitCode::SUCCESS;
            }
            "--help" | "-h" => {
                print_help();
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!(
                    "coding-runner: unexpected argument {other:?}; a Pod passes the job in {SPEC_ENV}",
                );
                print_help();
                return ExitCode::from(2);
            }
        }
    }

    let spec = match RunnerSpec::from_env() {
        Ok(spec) => spec,
        Err(err) => {
            let message = err.to_string();
            let written =
                emit(&RunnerResult::new(Status::InvalidSpec, message.clone()).with_error(message));
            return outcome(written);
        }
    };

    let mut cancel = match Cancellation::new() {
        Ok(cancel) => cancel,
        Err(err) => {
            let message = format!("could not subscribe to SIGTERM: {err}");
            let written =
                emit(&RunnerResult::new(Status::Failed, message.clone()).with_error(message));
            return outcome(written);
        }
    };

    let result = exec::run(&spec, &mut cancel).await;
    outcome(emit(&result))
}

/// Exit 0 whenever a result line was written: a failing check is data, not a runner crash.
fn outcome(written: bool) -> ExitCode {
    if written {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn print_help() {
    println!(
        "coding-runner {version}\n\n\
         In a Pod: reads {SPEC_ENV} (JSON) and writes one line {{\"coding_result\": …}} to stdout.\n\
         \n\
         Usage:\n  \
         coding-runner            run the job in {SPEC_ENV}\n  \
         coding-runner --version  print the version (image smoke test)\n",
        version = env!("CARGO_PKG_VERSION"),
    );
}
