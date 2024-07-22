use crate::cmd::{get_command_completer, run_from, APP_NAME};
use crate::commons::CommandCompleter;
use log::error;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::{Highlighter, MatchingBracketHighlighter};
use rustyline::hint::{Hinter, HistoryHinter};
use rustyline::validate::{MatchingBracketValidator, Validator};
use rustyline::{validate, CompletionType, Config, Context, Editor};
use rustyline_derive::Helper;
use shellwords::split;
use std::borrow::Cow::{self, Borrowed, Owned};
use std::sync::atomic::AtomicBool;

pub static INTERACT_STATUS: AtomicBool = AtomicBool::new(false);

#[derive(Helper)]
struct MyHelper {
    completer: CommandCompleter,
    highlighter: MatchingBracketHighlighter,
    validator: MatchingBracketValidator,
    hinter: HistoryHinter,
    colored_prompt: String,
}

impl Completer for MyHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        self.completer.complete(line, pos, ctx)
    }
}

impl Hinter for MyHelper {
    type Hint = String;
    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<String> {
        self.hinter.hint(line, pos, ctx);
        Some("".to_string())
    }
}

impl Highlighter for MyHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default {
            Borrowed(&self.colored_prompt)
        } else {
            Borrowed(prompt)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Owned("\x1b[1m".to_owned() + hint + "\x1b[m")
    }

    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_char(&self, line: &str, pos: usize, forced: bool) -> bool {
        self.highlighter.highlight_char(line, pos, forced)
    }
}

impl Validator for MyHelper {
    fn validate(
        &self,
        ctx: &mut validate::ValidationContext,
    ) -> rustyline::Result<validate::ValidationResult> {
        self.validator.validate(ctx)
    }

    fn validate_while_typing(&self) -> bool {
        self.validator.validate_while_typing()
    }
}

pub fn run() {
    let config = Config::builder()
        .history_ignore_space(true)
        .completion_type(CompletionType::List)
        .build();

    let h = MyHelper {
        completer: get_command_completer(),
        highlighter: MatchingBracketHighlighter::new(),
        hinter: HistoryHinter {},
        colored_prompt: "".to_owned(),
        validator: MatchingBracketValidator::new(),
    };

    let mut rl = match Editor::with_config(config) {
        Ok(rl) => rl,
        Err(e) => {
            log::error!("{}", e);
            return;
        }
    };

    rl.set_helper(Some(h));

    if rl.load_history("./.history").is_err() {
        println!("No previous history.");
    }

    INTERACT_STATUS.store(true, std::sync::atomic::Ordering::SeqCst);

    loop {
        let p = format!("{}> ", APP_NAME);
        rl.helper_mut().expect("No helper").colored_prompt = format!("\x1b[1;32m{}\x1b[0m", p);
        let readline = rl.readline(&p);
        match readline {
            Ok(line) => {
                if line.trim_start().is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(line.as_str());
                match split(line.as_str()).as_mut() {
                    Ok(arg) => {
                        if arg[0] == "exit" {
                            println!("bye!");
                            break;
                        }
                        arg.insert(0, "".to_string());
                        run_from(arg.to_vec())
                    }
                    Err(err) => {
                        println!("{}", err)
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.append_history("./.history")
        .map_err(|err| error!("{}", err))
        .ok();
}
