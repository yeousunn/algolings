use algolings_cli::{
    acquire_watch_lock, filter_test_output, has_shown_welcome, mark_welcome_shown, render_plain,
    run_interactive, run_multi_exercise_loop, run_trace, running_indicator, welcome_screen,
    HintTracker, LockError, Module, MultiExerciseState, StepOutcome, TraceError, MODULES,
};
use crossterm::style::Stylize;
use std::io::IsTerminal;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const DEBOUNCE_PERIOD: Duration = Duration::from_millis(250);
const TRACE_TIMEOUT: Duration = Duration::from_secs(5);
const TEST_TIMEOUT: Duration = Duration::from_secs(30);

fn main() {
    // --plain: sequential-text fallback for screen readers/non-TTY use
    // (design review Accessibility pass). Also auto-selected when stdout
    // isn't a real terminal, since the ratatui TUI can't render there.
    // Also means: no ANSI color, ever, in this mode.
    let plain_mode =
        std::env::args().any(|arg| arg == "--plain") || !std::io::stdout().is_terminal();

    let workspace_root = std::env::current_dir().expect("failed to read current directory");

    let _lock = match acquire_watch_lock(&workspace_root) {
        Ok(lock) => lock,
        Err(LockError::AlreadyRunning) => {
            eprintln!(
                "algolings is already watching this repo in another terminal — \
                 only one `algolings watch` can run at a time."
            );
            std::process::exit(1);
        }
        Err(LockError::Io(err)) => {
            eprintln!("failed to acquire the watch lock: {err}");
            std::process::exit(1);
        }
    };

    if !has_shown_welcome(&workspace_root) {
        let total_exercises: usize = MODULES.iter().map(|m| m.exercises.len()).sum();
        print!("{}", welcome_screen(total_exercises));
        if let Err(err) = mark_welcome_shown(&workspace_root) {
            eprintln!("note: could not persist first-run marker: {err}");
        }
    }

    // Hints are requested by typing "h" + Enter, read on a background
    // thread — not a raw single-keypress, which would require enabling
    // terminal raw mode for the whole session (the trace view's step/
    // auto-play controls need that, but they're already self-contained
    // inside run_interactive; hints aren't latency-sensitive enough to be
    // worth the same complexity here).
    let hint_tracker = Arc::new(Mutex::new(HintTracker::new()));
    spawn_hint_listener(hint_tracker.clone(), plain_mode);

    for (i, module) in MODULES.iter().enumerate() {
        let next_module_name = MODULES.get(i + 1).map(|m| m.name);
        let result = run_module(
            &workspace_root,
            module,
            hint_tracker.clone(),
            plain_mode,
            next_module_name,
        );
        if let Err(err) = result {
            eprintln!("watch error: {err}");
            std::process::exit(1);
        }
    }
}

/// Runs one module (e.g. sorting, searching) to completion: watches its
/// directory, runs its package's tests on every save, replays a passing
/// exercise's trace, and reports pass/fail — generalizing what used to be
/// main()'s only body, back when there was only ever one module.
fn run_module(
    workspace_root: &Path,
    module: &'static Module,
    hint_tracker: Arc<Mutex<HintTracker>>,
    plain_mode: bool,
    next_module_name: Option<&'static str>,
) -> notify::Result<()> {
    let mut state = MultiExerciseState::new(module.exercises);
    let hint_tracker_step = hint_tracker.clone();
    let hint_tracker_ready = hint_tracker.clone();

    run_multi_exercise_loop(
        workspace_root,
        &workspace_root.join(module.watch_dir),
        module.package,
        &mut state,
        DEBOUNCE_PERIOD,
        TEST_TIMEOUT,
        None,
        move |exercise| {
            // Seed the hint tracker here too, not just on a failing live
            // save (ExerciseFailed below) — this fires right after catch_up
            // resolves the current exercise on startup/resume, and again
            // whenever progress advances to a new exercise, both of which
            // otherwise leave HintTracker's `current` at None until the
            // learner's first save. Pressing [h] in that window hit the
            // same `None` branch as genuinely-exhausted hints, so a
            // freshly-arrived exercise misreported "no more hints" instead
            // of offering its first one.
            hint_tracker_ready
                .lock()
                .unwrap()
                .set_current_exercise(exercise);
            println!("watching {}", exercise.skeleton_path);
        },
        || print!("{}", running_indicator()),
        move |step| match step {
            StepOutcome::ExerciseFailed { exercise, outcome } => {
                hint_tracker_step
                    .lock()
                    .unwrap()
                    .set_current_exercise(exercise);
                let current_file = file_name(exercise.skeleton_path);
                let cleaned = filter_test_output(&outcome.output, current_file);

                print_status_line(plain_mode, exercise.name, false);
                println!("{cleaned}");
                print_hint_prompt(plain_mode);
            }
            StepOutcome::ExercisePassed { exercise, .. } => {
                hint_tracker_step.lock().unwrap().clear();
                match run_trace(
                    workspace_root,
                    module.package,
                    exercise.trace_key,
                    exercise.fixture,
                    exercise.target,
                    TRACE_TIMEOUT,
                ) {
                    Ok(events) => {
                        if let Some(value) = exercise.target {
                            print_value_banner(value);
                        }
                        // Exercises whose events themselves ADD the fixture's
                        // values (e.g. insert) must start from an empty
                        // picture — starting from fixture and then replaying
                        // inserts of those same values would double them.
                        let starting_array: &[i32] =
                            if exercise.starts_empty { &[] } else { exercise.fixture };
                        if plain_mode {
                            println!("{}", render_plain(starting_array, &events));
                        } else if let Err(err) = run_interactive(
                            starting_array,
                            events,
                            exercise.name,
                            exercise.target,
                        ) {
                            eprintln!("trace renderer error: {err}");
                        }
                    }
                    Err(TraceError::Panicked(msg)) => {
                        println!(
                            "note: the trace replay hit a problem after tests passed:\n{msg}"
                        );
                    }
                    Err(TraceError::TimedOut) => {
                        println!(
                            "note: the trace replay timed out (possible infinite loop) — \
                             tests still passed, so this is unexpected."
                        );
                    }
                    Err(TraceError::Io(err)) => {
                        eprintln!("trace replay error: {err}");
                    }
                }

                print_status_line(plain_mode, exercise.name, true);
                print_concept_note(plain_mode, exercise.concept_note);
            }
        },
        move || {
            hint_tracker.lock().unwrap().clear();
            let total_exercises: usize = MODULES.iter().map(|m| m.exercises.len()).sum();
            let message = completion_message(module.name, next_module_name, total_exercises);
            if plain_mode {
                println!("\n{message}");
            } else {
                println!("\n{}", message.green().bold());
            }
        },
    )
}

/// The message printed once a module's exercises are all solved: announces
/// the next module if one remains, or final completion on the last one.
fn completion_message(
    module_name: &str,
    next_module_name: Option<&str>,
    total_exercises: usize,
) -> String {
    match next_module_name {
        Some(next) => format!("{module_name} exercises complete! Moving on to {next}..."),
        None => format!("All {total_exercises} exercises complete! Nice work."),
    }
}

/// The bare file name (e.g. "bubble.rs") from a skeleton path like
/// "exercises/sort/src/bubble.rs" — matches how it appears in cargo's
/// diagnostic `--> path:line:col` lines, for `filter_test_output`.
fn file_name(skeleton_path: &str) -> &str {
    skeleton_path.rsplit('/').next().unwrap_or(skeleton_path)
}

fn print_status_line(plain_mode: bool, exercise_name: &str, passed: bool) {
    let label = if passed { "PASSED" } else { "FAILED" };
    let line = format!("{exercise_name} — {label}");
    if plain_mode {
        println!("\n{line}");
    } else if passed {
        println!("\n{}", line.green().bold());
    } else {
        println!("\n{}", line.red().bold());
    }
}

fn print_value_banner(value: i32) {
    println!("value: {value}");
}

fn print_hint_prompt(plain_mode: bool) {
    if plain_mode {
        println!("[h] show hint (type h and press enter)");
    } else {
        println!("{}", "[h] show hint".cyan().bold());
    }
}

fn print_concept_note(plain_mode: bool, note: &str) {
    if plain_mode {
        println!("{note}");
    } else {
        println!("{} {note}", "lesson:".magenta().bold());
    }
}

#[cfg(test)]
mod tests {
    use super::{completion_message, file_name};

    #[test]
    fn extracts_the_bare_file_name_from_a_skeleton_path() {
        assert_eq!(file_name("exercises/sort/src/bubble.rs"), "bubble.rs");
    }

    #[test]
    fn returns_the_input_unchanged_if_there_is_no_slash() {
        assert_eq!(file_name("bubble.rs"), "bubble.rs");
    }

    #[test]
    fn completion_message_announces_the_next_module_when_one_remains() {
        let message = completion_message("sorting", Some("searching"), 10);
        assert!(message.contains("sorting"));
        assert!(message.contains("searching"));
    }

    #[test]
    fn completion_message_announces_final_completion_on_the_last_module() {
        let message = completion_message("searching", None, 10);
        assert!(message.contains("10"));
        assert!(message.to_lowercase().contains("complete"));
        assert!(!message.contains("searching"));
    }
}

fn spawn_hint_listener(hint_tracker: Arc<Mutex<HintTracker>>, plain_mode: bool) {
    std::thread::spawn(move || {
        use std::io::BufRead;
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else { break };
            if line.trim() != "h" {
                continue;
            }
            let mut tracker = hint_tracker.lock().unwrap();
            match tracker.next_hint() {
                Some(hint) => {
                    if plain_mode {
                        println!("hint: {hint}");
                    } else {
                        println!("{} {hint}", "hint:".yellow().bold());
                    }
                }
                None => println!(
                    "no more hints for this exercise (or nothing to hint about right now)"
                ),
            }
        }
    });
}
