use _lib::{symdel_across_sets, symdel_within_set};
use clap::{ArgAction, Parser};
use rayon::ThreadPoolBuilder;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Error, ErrorKind, Write};

/// Minimal CLI utility for fast detection of nearest neighbour strings that fall within a
/// threshold edit distance.
///
/// If you provide nearust with a path to a [FILE_PRIMARY], it will read its contents for input. If
/// no path is supplied, nearust will read from the standard input until it receives an EOF signal.
/// Nearust will then look for pairs of similar strings within its input, where each line of text
/// is treated as an individual string. You can also supply nearust with two paths -- a
/// [FILE_PRIMARY] and [FILE_COMPARISON], in which case the program will look for pairs of similar
/// strings across the contents of the two files. Currently, only valid ASCII input is supported.
///
/// By default, the threshold (Levenshtein) edit distance at or below which a pair of strings are
/// considered similar is set at 1. This can be changed by setting the --max-distance option.
///
/// Nearust's output is plain text, where each line encodes a detected pair of similar input
/// strings. Each line is comprised of three integers separated by commas, which represent, in
/// respective order: the (1-indexed) line number of the string from the primary input (i.e. stdin
/// or [FILE_PRIMARY]), the (1-indexed) line number of the string from the secondary input (i.e.
/// stdin or [FILE_PRIMARY] if one input, or [FILE_COMPARISON] if two inputs), and the
/// (Levenshtein) edit distance between the similar strings.
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// The maximum (Levenshtein) edit distance away to check for neighbours.
    #[arg(short = 'd', long, default_value_t = 1)]
    max_distance: usize,

    /// The number of OS threads the program spawns (if 0 spawns one thread per CPU core).
    #[arg(short, long, default_value_t = 0)]
    num_threads: usize,

    /// 0-index line numbers in the output.
    #[arg(short, long, action = ArgAction::SetTrue)]
    zero_index: bool,

    /// Primary input file (if absent program reads from stdin until EOF).
    file_primary: Option<String>,

    /// If provided, searches for pairs of similar strings between the primary input file and the
    /// comparison input file.
    file_comparison: Option<String>,
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
        .unwrap_or_else(|_| panic!("global thread pool cannot be initialised more than once"));

    let primary_input = match args.file_primary {
        Some(path) => {
            let reader = get_file_bufreader(&path);
            get_input_lines_as_ascii(reader)
                .unwrap_or_else(|e| panic!("(from {}) {}", &path, e.to_string()))
        }
        None => {
            let stdin = io::stdin().lock();
            get_input_lines_as_ascii(stdin)
                .unwrap_or_else(|e| panic!("(from stdin) {}", e.to_string()))
        }
    };

    let results = match args.file_comparison {
        Some(path) => {
            let comparison_reader = get_file_bufreader(&path);
            let comparison_input = get_input_lines_as_ascii(comparison_reader)
                .unwrap_or_else(|e| panic!("(from {}) {}", &path, e.to_string()));

            symdel_across_sets(
                &primary_input,
                &comparison_input,
                args.max_distance,
                args.zero_index,
            )
        }
        None => symdel_within_set(&primary_input, args.max_distance, args.zero_index),
    };
    write_results(results, &mut stdout);
}

/// Write to stdout
fn write_results(results: Vec<(usize, usize, usize)>, writer: &mut impl Write) {
    for (a_idx_to_write, c_idx_to_write, dist) in results.iter() {
        write!(writer, "{},{},{}\n", a_idx_to_write, c_idx_to_write, dist).unwrap();
    }
}

/// Get a buffered reader to a file at path.
fn get_file_bufreader(path: &str) -> BufReader<File> {
    let file =
        File::open(&path).unwrap_or_else(|e| panic!("failed to open {}: {}", &path, e.to_string()));
    BufReader::new(file)
}

/// Read lines from in_stream until EOF and collect into vector of byte vectors. Return any
/// errors if trouble reading, or if the input text contains non-ASCII data. The returned vector
/// is guaranteed to only contain ASCII bytes.
fn get_input_lines_as_ascii(in_stream: impl BufRead) -> Result<Vec<String>, Error> {
    let mut strings = Vec::new();

    for (idx, line) in in_stream.lines().enumerate() {
        let line_unwrapped = line?;

        if !line_unwrapped.is_ascii() {
            let err_msg = format!(
                "non-ASCII data is currently unsupported (\"{}\" from input line {})",
                line_unwrapped,
                idx + 1
            );
            return Err(Error::new(ErrorKind::InvalidData, err_msg));
        }

        strings.push(line_unwrapped);
    }

    Ok(strings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use io::Read;

    #[test]
    fn test_get_input_lines_as_ascii() {
        let strings = get_input_lines_as_ascii(&mut "foo\nbar\nbaz\n".as_bytes()).unwrap();
        let expected: Vec<String> = vec!["foo".into(), "bar".into(), "baz".into()];
        assert_eq!(strings, expected);
    }

    /// Run this test from the project home directory so that the test CDR3 text files can be found
    /// at the expected paths
    #[test]
    fn test_within() {
        let f = BufReader::new(File::open("test_files/cdr3b_10k_a.txt").unwrap());
        let test_input = get_input_lines_as_ascii(f).unwrap();

        let mut f = BufReader::new(File::open("test_files/results_10k_a.txt").unwrap());
        let mut expected_output = Vec::new();
        let _ = f.read_to_end(&mut expected_output);

        let mut test_output_stream = Vec::new();

        let results = symdel_within_set(&test_input, 1, false);
        write_results(results, &mut test_output_stream);

        assert_eq!(test_output_stream, expected_output);
    }

    /// Run this test from the project home directory so that the test CDR3 text files can be found
    /// at the expected paths
    #[test]
    fn test_cross() {
        let f = BufReader::new(File::open("test_files/cdr3b_10k_a.txt").unwrap());
        let primary_input = get_input_lines_as_ascii(f).unwrap();

        let f = BufReader::new(File::open("test_files/cdr3b_10k_b.txt").unwrap());
        let comparison_input = get_input_lines_as_ascii(f).unwrap();

        let mut f = BufReader::new(File::open("test_files/results_10k_cross.txt").unwrap());
        let mut expected_output = Vec::new();
        let _ = f.read_to_end(&mut expected_output);

        let mut test_output_stream = Vec::new();

        let results = symdel_across_sets(&primary_input, &comparison_input, 1, false);
        write_results(results, &mut test_output_stream);

        assert_eq!(test_output_stream, expected_output);
    }
}
