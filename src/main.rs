use std::io;
use std::io::{Read, Write};

fn main() -> Result<(), io::Error> {
    nearust(&mut io::stdin(), &mut io::stdout(), &mut io::stderr())
}

fn nearust(
    in_stream: &mut impl Read, 
    out_stream: &mut impl Write, 
    _err_stream: &mut impl Write
) -> Result<(), io::Error> {
    // Reads (blocking) all lines from in_stream until EOF, and converts the data into a vector of
    // Strings where each String is a line from in_stream. Performs symdel to look for String
    // pairs within 1 edit distance. Outputs the detected pairs from symdel into out_stream, where
    // each new line written encodes a detected pair as a pair of 0-indexed indices of the Strings
    // involved separated by a comma, and the lower index is always first.
    //
    // Any unrecoverable errors should be written out to err_stream, before the program exits.
    //
    // The function accepts the three aforementioned streams as parameters instead of having them
    // directly bound to stdin, stdout and stderr respectively. This is so that the streams can be
    // easily bound to other buffers for the purposes of testing.
    //
    // The error stream is currently unused as we do not handle any errors, and thus its name is
    // prefixed with an underscore.

    let mut in_buffer = "".to_string();
    in_stream.read_to_string(&mut in_buffer)?;
    let _ = out_stream.write("1,2".as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_test() {
        // This below is not meant to be a real test of code functionality, but is just here to
        // give us somewhere to start, and to make sure the testing boilerplate is ready.

        let mut out = Vec::new();
        let mut err = Vec::new();
        nearust(&mut "foo\nbar\nbaz".as_bytes(), &mut out, &mut err).unwrap();
        assert_eq!(out, b"1,2");
    }
}
