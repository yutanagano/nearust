use std::io;
use std::io::{Read, Write};

fn main() -> Result<(), io::Error> {
    nearust(&mut io::stdin(), &mut io::stdout(), &mut io::stderr())
}

fn nearust(
    stdin: &mut impl Read, 
    stdout: &mut impl Write, 
    _stderr: &mut impl Write
) -> Result<(), io::Error> {
    let mut in_buffer = "".to_string();
    stdin.read_to_string(&mut in_buffer)?;
    let _ = stdout.write("1,2".as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_test() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        nearust(&mut "foo\nbar\nbaz".as_bytes(), &mut out, &mut err).unwrap();
        assert_eq!(out, b"1,2");
    }
}
