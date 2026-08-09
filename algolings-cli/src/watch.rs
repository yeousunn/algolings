//! Debounce and stale-run-cancellation primitives for the file watcher
//! (Architecture review issues 1-2, eng review). Kept decoupled from the
//! actual `notify` filesystem events so the timing/coalescing logic is
//! unit-testable without touching a real filesystem.

use crate::progress::{MultiExerciseState, StepOutcome};
use crate::test_runner::run_package_tests;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;

/// Tracks which "generation" of file-change is current, so a background
/// test/replay run started for an older generation can tell it's been
/// superseded and its result should be discarded rather than displayed.
pub struct Debouncer {
    generation: AtomicU64,
}

impl Debouncer {
    pub fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
        }
    }

    /// Call when a settled (debounced) file change is observed. Returns a
    /// token identifying this generation.
    pub fn bump(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// True if `token` is still the most recent generation.
    pub fn is_current(&self, token: u64) -> bool {
        self.generation.load(Ordering::SeqCst) == token
    }
}

impl Default for Debouncer {
    fn default() -> Self {
        Self::new()
    }
}

/// Blocks until `rx` has been silent for `quiet_period`, draining and
/// coalescing any events that arrive in the meantime — so several rapid
/// saves (common with editors that fire multiple filesystem events per
/// save) settle into a single trigger instead of one per event.
pub fn wait_for_quiet(rx: &Receiver<()>, quiet_period: Duration) -> Result<(), RecvTimeoutError> {
    // Block for at least one event before starting the quiet-period clock.
    rx.recv().map_err(|_| RecvTimeoutError::Disconnected)?;
    loop {
        match rx.recv_timeout(quiet_period) {
            Ok(()) => continue,
            // Timeout means genuinely quiet. Disconnected-after-an-event
            // means the sender is done (e.g. the watcher shut down) — from
            // the caller's perspective both mean "no more events coming,
            // safe to act now", so both settle successfully.
            Err(RecvTimeoutError::Timeout) => return Ok(()),
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        }
    }
}

/// Watches `path` for filesystem changes, forwarding a `()` signal for each
/// notify event. The returned watcher must be kept alive for as long as
/// watching should continue — dropping it stops the underlying OS watch.
pub fn watch_path(path: &Path) -> notify::Result<(RecommendedWatcher, Receiver<()>)> {
    let (tx, rx) = mpsc::channel();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if res.is_ok() {
            let _ = tx.send(());
        }
    })?;
    watcher.watch(path, RecursiveMode::NonRecursive)?;
    Ok((watcher, rx))
}

/// The core `algolings watch` loop: watches `watch_dir` (the directory
/// holding all exercise skeletons), debounces saves, and on each settle
/// checks the CURRENT exercise (per `state`, sequential progression —
/// design decision from step 4) — reporting pass/fail and advancing state,
/// with results superseded by a newer save discarded before they complete.
///
/// Before entering the loop, fast-forwards `state` past any exercises that
/// already pass (e.g. a returning learner who solved some in a prior
/// session), so the correct "current" exercise is known immediately.
///
/// `max_iterations`: `None` runs forever (the real CLI); `Some(n)` stops
/// after `n` settled checks (used by tests, which can't block forever).
///
/// `on_current_exercise` fires with the exercise the learner is now on —
/// once right after catch-up (when the module isn't already complete), and
/// again every time progress advances to a new exercise. `on_all_complete`
/// covers the "nothing left to announce" case instead.
#[allow(clippy::too_many_arguments)]
pub fn run_multi_exercise_loop(
    workspace_root: &Path,
    watch_dir: &Path,
    package: &str,
    state: &mut MultiExerciseState,
    quiet_period: Duration,
    test_timeout: Duration,
    max_iterations: Option<u32>,
    mut on_current_exercise: impl FnMut(&'static crate::exercise::Exercise),
    mut on_settled: impl FnMut(),
    mut on_step: impl FnMut(StepOutcome),
    mut on_all_complete: impl FnMut(),
) -> notify::Result<()> {
    // Watch BEFORE the (potentially slow, subprocess-running) catch_up
    // scan below, not after — otherwise a save landing during catch_up
    // would happen before the watcher exists and be missed entirely,
    // leaving the loop's first wait_for_quiet() blocked forever. Events
    // that arrive during catch_up just sit in the channel and get drained
    // by the first wait_for_quiet() call once the loop starts.
    let (_watcher, rx) = watch_path(watch_dir)?;

    state.catch_up(workspace_root, package, test_timeout).ok();
    if state.is_complete() {
        on_all_complete();
        return Ok(());
    }
    on_current_exercise(state.current().expect("checked is_complete above"));

    let debouncer = Debouncer::new();

    let mut ran = 0;
    loop {
        if wait_for_quiet(&rx, quiet_period).is_err() {
            break;
        }
        on_settled();
        let Some(exercise) = state.current() else {
            break;
        };
        let token = debouncer.bump();
        if let Ok(outcome) =
            run_package_tests(workspace_root, package, exercise.test_filter, test_timeout)
            && debouncer.is_current(token)
        {
            let passed = outcome.passed;
            on_step(state.check(outcome));
            if state.is_complete() {
                on_all_complete();
                return Ok(());
            } else if passed {
                on_current_exercise(state.current().expect("checked is_complete above"));
            }
        }
        ran += 1;
        if max_iterations.is_some_and(|max| ran >= max) {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    const A_GENEROUS_TIMEOUT: Duration = Duration::from_secs(30);

    #[test]
    fn fresh_token_is_current() {
        let debouncer = Debouncer::new();
        let token = debouncer.bump();
        assert!(debouncer.is_current(token));
    }

    #[test]
    fn superseded_token_is_no_longer_current() {
        let debouncer = Debouncer::new();
        let stale_token = debouncer.bump();
        let _fresh_token = debouncer.bump();
        assert!(!debouncer.is_current(stale_token));
    }

    #[test]
    fn wait_for_quiet_returns_after_a_single_event_settles() {
        let (tx, rx) = mpsc::channel();
        tx.send(()).unwrap();
        let result = wait_for_quiet(&rx, Duration::from_millis(30));
        assert!(result.is_ok());
    }

    #[test]
    fn wait_for_quiet_coalesces_rapid_events_into_one_settle() {
        let (tx, rx) = mpsc::channel();
        // Keep a sender alive in this thread for the whole test, so the
        // channel disconnecting (once the spawned thread finishes) doesn't
        // interfere with measuring the pure quiet-period debounce behavior.
        let _tx_keep_alive = tx.clone();
        thread::spawn(move || {
            for _ in 0..5 {
                tx.send(()).unwrap();
                thread::sleep(Duration::from_millis(5));
            }
        });
        let start = std::time::Instant::now();
        wait_for_quiet(&rx, Duration::from_millis(40)).unwrap();
        let elapsed = start.elapsed();
        // 5 events, 5ms apart (~20ms total), then a 40ms quiet window: the
        // settle should land around ~60ms, not fire on the first event
        // (~40ms) or take drastically longer.
        assert!(elapsed >= Duration::from_millis(40));
        assert!(elapsed < Duration::from_millis(200));
    }

    #[test]
    fn wait_for_quiet_errors_when_sender_is_dropped_before_any_event() {
        let (tx, rx) = mpsc::channel::<()>();
        drop(tx);
        assert!(wait_for_quiet(&rx, Duration::from_millis(10)).is_err());
    }

    #[test]
    fn wait_for_quiet_settles_promptly_if_sender_disconnects_after_an_event() {
        // Models the watcher shutting down mid-debounce: once at least one
        // event has been seen, a disconnect should settle immediately
        // rather than waiting out the full quiet period or erroring.
        let (tx, rx) = mpsc::channel();
        tx.send(()).unwrap();
        drop(tx);
        let start = std::time::Instant::now();
        let result = wait_for_quiet(&rx, Duration::from_millis(200));
        assert!(result.is_ok());
        assert!(start.elapsed() < Duration::from_millis(200));
    }

    #[test]
    fn watch_path_reports_a_real_file_write() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("watched.rs");
        std::fs::write(&file_path, "initial").unwrap();

        let (_watcher, rx) = watch_path(&file_path).unwrap();
        // Give the OS watch a moment to fully register before writing.
        thread::sleep(Duration::from_millis(50));
        std::fs::write(&file_path, "changed").unwrap();

        let result = rx.recv_timeout(Duration::from_secs(2));
        assert!(result.is_ok(), "expected a filesystem event within 2s");
    }

    fn workspace_root() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf()
    }

    #[test]
    fn already_all_solved_completes_immediately_via_catch_up() {
        // Points at sort-solutions, where every exercise already passes —
        // proves catch_up() fast-forwards a returning learner straight to
        // AllComplete without needing any file event at all.
        let dir = tempfile::tempdir().unwrap();
        let mut state = MultiExerciseState::new(crate::exercise::SORT_EXERCISES);
        let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let completed_clone = completed.clone();

        run_multi_exercise_loop(
            &workspace_root(),
            dir.path(),
            "sort-solutions",
            &mut state,
            Duration::from_millis(30),
            A_GENEROUS_TIMEOUT,
            Some(0),
            |_| panic!("on_ready should not fire when already complete after catch_up"),
            || {},
            |_| panic!("should complete via catch_up before checking any step"),
            move || completed_clone.store(true, std::sync::atomic::Ordering::SeqCst),
        )
        .unwrap();

        assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
        assert!(state.is_complete());
    }

    #[test]
    fn all_unsolved_stays_on_the_first_exercise_and_never_completes() {
        // Points at exercises-sort, where every skeleton is still
        // unsolved — proves the loop stays on exercise 0 and reports
        // ExerciseFailed on each settle rather than silently advancing.
        let dir = tempfile::tempdir().unwrap();
        let watched_file = dir.path().join("watched.rs");
        std::fs::write(&watched_file, "initial").unwrap();

        let mut state = MultiExerciseState::new(crate::exercise::SORT_EXERCISES);
        let failed_names = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let failed_names_clone = failed_names.clone();
        let ready_name = std::sync::Arc::new(std::sync::Mutex::new(None));
        let ready_name_clone = ready_name.clone();

        let handle = std::thread::spawn(move || {
            run_multi_exercise_loop(
                &workspace_root(),
                dir.path(),
                "exercises-sort",
                &mut state,
                Duration::from_millis(30),
                A_GENEROUS_TIMEOUT,
                Some(1),
                move |exercise| *ready_name_clone.lock().unwrap() = Some(exercise.name),
                || {},
                move |step| {
                    if let StepOutcome::ExerciseFailed { exercise, .. } = step {
                        failed_names_clone.lock().unwrap().push(exercise.name);
                    } else {
                        panic!("expected ExerciseFailed for an unsolved skeleton");
                    }
                },
                || panic!("should not complete while every exercise is unsolved"),
            )
            .unwrap();
            assert!(!state.is_complete());
            assert_eq!(state.current().map(|e| e.name), Some("bubble_sort"));
        });

        thread::sleep(Duration::from_millis(100));
        std::fs::write(&watched_file, "changed").unwrap();
        handle.join().unwrap();

        assert_eq!(*failed_names.lock().unwrap(), vec!["bubble_sort"]);
        assert_eq!(*ready_name.lock().unwrap(), Some("bubble_sort"));
    }

    #[test]
    fn on_current_exercise_can_seed_a_hint_tracker_before_any_save() {
        // Regression test for a real bug: HintTracker was only ever seeded
        // from a failing live-save result (ExerciseFailed), never from
        // on_current_exercise. That left a window — most visibly right
        // after catch_up resolves the current exercise on a restart,
        // before the learner has touched a file — where pressing [h] hit
        // the same `None` branch as genuinely-exhausted hints, misreporting
        // "no more hints" for an exercise the learner hadn't even seen a
        // hint for yet. Wires a real HintTracker through on_current_exercise
        // the same way main.rs does, and checks a hint is available the
        // moment catch_up lands on a failing exercise — no save required.
        let dir = tempfile::tempdir().unwrap();
        let watched_file = dir.path().join("watched.rs");
        std::fs::write(&watched_file, "initial").unwrap();

        let mut state = MultiExerciseState::new(crate::exercise::SORT_EXERCISES);
        let hint_tracker = std::sync::Arc::new(std::sync::Mutex::new(
            crate::hints::HintTracker::new(),
        ));
        let hint_tracker_ready = hint_tracker.clone();
        let (ready_tx, ready_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            run_multi_exercise_loop(
                &workspace_root(),
                dir.path(),
                "exercises-sort",
                &mut state,
                Duration::from_millis(30),
                A_GENEROUS_TIMEOUT,
                Some(1),
                move |exercise| {
                    hint_tracker_ready
                        .lock()
                        .unwrap()
                        .set_current_exercise(exercise);
                    ready_tx.send(()).unwrap();
                },
                || {},
                |_| {},
                || panic!("should not complete while every exercise is unsolved"),
            )
            .unwrap();
        });

        ready_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("on_current_exercise should fire once catch_up resolves the exercise");
        assert_eq!(
            hint_tracker.lock().unwrap().next_hint(),
            Some(crate::exercise::SORT_EXERCISES[0].hints[0]),
            "a hint should be available immediately after catch_up, with no save yet"
        );

        // Let the loop settle and finish so the spawned thread cleans up.
        std::fs::write(&watched_file, "changed").unwrap();
        handle.join().unwrap();
    }

    /// A standalone throwaway crate whose single test reads its pass/fail
    /// result from a `flag.txt` file at runtime — so two `cargo test`
    /// invocations against the SAME compiled binary can observe different
    /// outcomes without recompiling or touching any real project file.
    fn build_flag_crate() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[package]\nname = \"flagpkg\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        std::fs::create_dir(dir.path().join("src")).unwrap();
        std::fs::write(
            dir.path().join("src/lib.rs"),
            "#[cfg(test)]\nmod tests {\n    #[test]\n    fn flag_check() {\n        \
             let flag_path = concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/flag.txt\");\n        \
             let content = std::fs::read_to_string(flag_path).unwrap_or_default();\n        \
             assert_eq!(content.trim(), \"pass\");\n    }\n}\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("flag.txt"), "fail").unwrap();
        dir
    }

    #[test]
    fn completing_the_last_exercise_via_a_live_save_returns_promptly() {
        // Regression test for a real bug: on_all_complete() fired when
        // completion happened INSIDE the loop (as opposed to via catch_up's
        // pre-loop early return), but the function never returned — it fell
        // through to the next wait_for_quiet() and blocked forever waiting
        // for a second save that would never come.
        let crate_dir = build_flag_crate();
        let watch_dir = tempfile::tempdir().unwrap();
        let watched_file = watch_dir.path().join("watched.rs");
        std::fs::write(&watched_file, "initial").unwrap();

        const FLAG_EXERCISE: &[crate::exercise::Exercise] = &[crate::exercise::Exercise {
            name: "flag",
            test_filter: "flag_check",
            trace_key: "flag_check",
            skeleton_path: "flag.txt",
            fixture: &[1],
            concept_note: "n/a",
            hints: &["hint"],
            target: None,
            starts_empty: false,
        }];
        let mut state = MultiExerciseState::new(FLAG_EXERCISE);

        let (done_tx, done_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let crate_root = crate_dir.path().to_path_buf();
        let watch_path_buf = watch_dir.path().to_path_buf();

        let handle = thread::spawn(move || {
            run_multi_exercise_loop(
                &crate_root,
                &watch_path_buf,
                "flagpkg",
                &mut state,
                Duration::from_millis(30),
                A_GENEROUS_TIMEOUT,
                None,
                // Fires once catch_up() has run and confirmed the module
                // is NOT yet complete — i.e. it genuinely observed "fail",
                // not a race where the flag flipped mid-catch_up.
                move |_| ready_tx.send(()).unwrap(),
                || {},
                |_| {},
                || {},
            )
            .unwrap();
            done_tx.send(()).unwrap();
        });

        // Wait for catch_up() to genuinely observe the flag failing before
        // flipping it — otherwise a race could let catch_up see "pass" and
        // complete via its own early-return path, which already works
        // correctly and wouldn't exercise the buggy in-loop path at all.
        ready_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("catch_up should observe the failing flag and start watching");
        std::fs::write(crate_dir.path().join("flag.txt"), "pass").unwrap();
        std::fs::write(&watched_file, "changed").unwrap();

        let result = done_rx.recv_timeout(Duration::from_secs(10));
        assert!(
            result.is_ok(),
            "run_multi_exercise_loop did not return after completing inside the loop \
             — it's stuck waiting for a second save that will never come"
        );
        handle.join().unwrap();
    }
}
