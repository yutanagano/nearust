use _lib::{
    get_candidates_cross, get_candidates_within, get_input_lines_as_ascii, write_true_results,
};
use clap::{ArgAction, Parser};
use rayon::ThreadPoolBuilder;
use std::fs::File;
use std::io::{self, BufReader, BufWriter};
use std::process;

/// Minimal CLI utility for fast detection of nearest neighbour strings that fall within a
/// threshold edit distance.
///
/// If you provide nearust with a path to a [FILE_QUERY], it will read its contents for input. If
/// no path is supplied, nearust will read from the standard input until it receives an EOF signal.
/// Nearust will then look for pairs of similar strings within its input, where each line of text
/// is treated as an individual string. You can also supply nearust with two paths -- a
/// [FILE_QUERY] and [FILE_REFERENCE], in which case the program will look for pairs of similar
/// strings across the contents of the two files. Currently, only valid ASCII input is supported.
///
/// By default, the threshold (Levenshtein) edit distance at or below which a pair of strings are
/// considered similar is set at 1. This can be changed by setting the --max-distance option.
///
/// Nearust's output is plain text, where each line encodes a detected pair of similar input
/// strings. Each line is comprised of three integers separated by commas, which represent, in
/// respective order: the (1-indexed) line number of the string from the primary input (i.e. stdin
/// or [FILE_QUERY]), the (1-indexed) line number of the string from the secondary input (i.e.
/// stdin or [FILE_QUERY] if one input, or [FILE_REFERENCE] if two inputs), and the (Levenshtein)
/// edit distance between the similar strings.
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// The maximum (Levenshtein) edit distance away to check for neighbours.
    #[arg(short = 'd', long, default_value_t = 1)]
    max_distance: u8,

    /// The number of OS threads the program spawns (if 0 spawns one thread per CPU core).
    #[arg(short, long, default_value_t = 0)]
    num_threads: usize,

    /// 0-index line numbers in the output.
    #[arg(short, long, action = ArgAction::SetTrue)]
    zero_index: bool,

    /// Primary input (if absent program reads from stdin until EOF).
    file_query: Option<String>,

    /// If provided, searches for pairs of similar strings between the query file and the reference
    /// file.
    file_reference: Option<String>,
}

/// Reads (blocking) all lines from in_stream until EOF, and converts the data into a vector of
/// Strings where each String is a line from in_stream. Performs symdel to look for String
/// pairs within <MAX_DISTANCE> (as read from the CLI arguments, defaults to 1) edit distance.
/// Outputs the detected pairs from symdel into out_stream, where each new line written encodes a
/// detected pair as a pair of 1-indexed line numbers of the input strings involved separated by a
/// comma, and the lower line number is always first.
fn main() {
    let mut stdout = BufWriter::new(io::stdout().lock());
    let args = Args::parse();

    ThreadPoolBuilder::new()
        .num_threads(args.num_threads)
        .build_global()
        .unwrap_or_else(|_| {
            eprintln!("global thread pool cannot be initialised more than once");
            process::exit(1);
        });

    let primary_input = match args.file_query {
        Some(path) => {
            let reader = get_file_bufreader(&path);
            get_input_lines_as_ascii(reader).unwrap_or_else(|e| {
                eprintln!("(from {}) {}", &path, e);
                process::exit(1);
            })
        }
        None => {
            let stdin = io::stdin().lock();
            get_input_lines_as_ascii(stdin).unwrap_or_else(|e| {
                eprintln!("(from stdin) {}", e);
                process::exit(1);
            })
        }
    };

    match args.file_reference {
        Some(path) => {
            let comparison_reader = get_file_bufreader(&path);
            let comparison_input =
                get_input_lines_as_ascii(comparison_reader).unwrap_or_else(|e| {
                    eprintln!("(from {}) {}", &path, e);
                    process::exit(1);
                });

            let hit_candidates =
                get_candidates_cross(&primary_input, &comparison_input, args.max_distance)
                    .unwrap_or_else(|e| {
                        eprintln!("{}", e);
                        process::exit(1)
                    });

            write_true_results(
                hit_candidates,
                &primary_input,
                &comparison_input,
                args.max_distance,
                args.zero_index,
                &mut stdout,
            );
        }
        None => {
            let hit_candidates = get_candidates_within(&primary_input, args.max_distance)
                .unwrap_or_else(|e| {
                    eprintln!("{}", e);
                    process::exit(1);
                });

            write_true_results(
                hit_candidates,
                &primary_input,
                &primary_input,
                args.max_distance,
                args.zero_index,
                &mut stdout,
            );
        }
    };
}

/// Get a buffered reader to a file at path.
fn get_file_bufreader(path: &str) -> BufReader<File> {
    let file = File::open(&path).unwrap_or_else(|e| {
        eprintln!("failed to open {}: {}", &path, e);
        process::exit(1)
    });
    BufReader::new(file)
}
