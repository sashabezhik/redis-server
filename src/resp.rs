use crate::resp_result::{RespError, RespLength, RespResult};

use std::fmt;

#[derive(Debug, PartialEq, Clone)]
pub enum Resp {
    Array(Vec<Self>),
    BulkString(String),
    Integer(i64),
    Null,
    RDBPrefix(usize),
    SimpleError(String),
    SimpleString(String),
}

impl Resp {
    pub fn simple_string(s: impl Into<String>) -> Self {
        Self::SimpleString(s.into())
    }

    pub fn simple_error(s: impl Into<String>) -> Self {
        Self::SimpleError(s.into())
    }

    pub fn bulk_string(s: impl Into<String>) -> Self {
        Self::BulkString(s.into())
    }

    pub fn bulk_array<I, T>(items: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        Self::Array(items.into_iter().map(Self::bulk_string).collect())
    }

    pub fn bulk_string_from_vec(strings: &[String]) -> Self {
        Self::BulkString(strings.join("\r\n"))
    }
}

impl fmt::Display for Resp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Array(data) => {
                write!(f, "*{}\r\n", data.len())?;
                for elem in data.iter() {
                    write!(f, "{}", elem)?;
                }
                Ok(())
            }
            Self::BulkString(data) => write!(f, "${}\r\n{}\r\n", data.len(), data),
            Self::Integer(data) => write!(f, ":{}\r\n", data),
            Self::Null => write!(f, "$-1\r\n"),
            Self::RDBPrefix(data) => write!(f, "${}\r\n", data),
            Self::SimpleError(data) => write!(f, "-{}\r\n", data),
            Self::SimpleString(data) => write!(f, "+{}\r\n", data),
        }
    }
}

// extracts bytes from the buffer until a `\r\n` is reached.
fn binary_extract_line(buffer: &[u8], index: &mut usize) -> RespResult<Vec<u8>> {
    let mut output = Vec::new();

    if *index >= buffer.len() {
        return Err(RespError::OutOfBounds(*index));
    }

    let mut previous_elem = buffer[*index];
    let mut separator_found = false;
    let mut final_index = *index;

    for &elem in buffer[*index..].iter() {
        final_index += 1;

        if elem == b'\n' && previous_elem == b'\r' {
            separator_found = true;
            break;
        }

        previous_elem = elem;
    }

    if !separator_found {
        *index = final_index;
        return Err(RespError::OutOfBounds(*index));
    }

    output.extend_from_slice(&buffer[*index..final_index - 2]);
    *index = final_index;

    Ok(output)
}

// extracts a given amount of bytes from the buffer.
fn binary_extract_bytes(buffer: &[u8], index: &mut usize, length: usize) -> RespResult<Vec<u8>> {
    let mut output = Vec::new();

    if *index + length > buffer.len() {
        return Err(RespError::OutOfBounds(buffer.len()));
    }

    output.extend_from_slice(&buffer[*index..*index + length]);

    *index += length;

    Ok(output)
}

// extracts bytes from the buffer until a `\r\n` is reached and converts them into a string.
pub fn binary_extract_line_as_string(buffer: &[u8], index: &mut usize) -> RespResult<String> {
    let line = binary_extract_line(buffer, index)?;

    Ok(String::from_utf8(line)?)
}

// checks that the first character of a RESP buffer is the given one and removes it.
pub fn resp_remove_type(value: char, buffer: &[u8], index: &mut usize) -> RespResult<()> {
    if buffer[*index] != value as u8 {
        return Err(RespError::WrongType);
    }
    *index += 1;

    Ok(())
}

// parse a simple string in the form `+VALUE\r\n`.
fn parse_simple_string(buffer: &[u8], index: &mut usize) -> RespResult<Resp> {
    resp_remove_type('+', buffer, index)?;

    let line = binary_extract_line_as_string(buffer, index)?;

    Ok(Resp::SimpleString(line))
}

// parse a bulk string in the form `$NUM\r\nVALUE\r\n`.
fn parse_bulk_string(buffer: &[u8], index: &mut usize) -> RespResult<Resp> {
    resp_remove_type('$', buffer, index)?;

    let length = resp_extract_length(buffer, index)?;

    if length == -1 {
        return Ok(Resp::Null);
    }

    if length < -1 {
        return Err(RespError::IncorrectLength(length));
    }

    let bytes = binary_extract_bytes(buffer, index, length as usize)?;

    let data = String::from_utf8(bytes)?;

    *index += 2;

    Ok(Resp::BulkString(data))
}

// parse an array in the form `*NUM\r\nELEM1\r\nELEM2\r\n...`.
fn parse_array(buffer: &[u8], index: &mut usize) -> RespResult<Resp> {
    resp_remove_type('*', buffer, index)?;

    let length = resp_extract_length(buffer, index)?;

    if length < 0 {
        return Err(RespError::IncorrectLength(length));
    }

    let mut data = Vec::new();

    for _ in 0..length {
        match parser_router(buffer, index) {
            Some(parse_func) => {
                let array_element = parse_func(buffer, index)?;

                data.push(array_element);
            }
            None => return Err(RespError::Unknown),
        }
    }

    Ok(Resp::Array(data))
}

// a function that parses a slice of bytes at the given index into a Resp value.
type RespParser = fn(&[u8], &mut usize) -> RespResult<Resp>;

// select the correct parsing function according to the data type found
// in the buffer at the given index.
fn parser_router(buffer: &[u8], index: &usize) -> Option<RespParser> {
    match buffer[*index] {
        b'+' => Some(parse_simple_string),
        b'$' => Some(parse_bulk_string),
        b'*' => Some(parse_array),
        _ => None,
    }
}

// parse the bytes of the buffer at the given index and return a RESP data type.
pub fn bytes_to_resp(buffer: &[u8], index: &mut usize) -> RespResult<Resp> {
    match parser_router(buffer, index) {
        Some(parse_func) => parse_func(buffer, index),
        None => Err(RespError::Unknown),
    }
}

// extracts a single line from a RESP buffer and interprets it as length.
// the type used for the number is RespLength.
pub fn resp_extract_length(buffer: &[u8], index: &mut usize) -> RespResult<RespLength> {
    let line = binary_extract_line_as_string(buffer, index)?;

    let length = line.parse()?;

    Ok(length)
}

#[cfg(test)]
mod tests {
    use super::*;

    // binary_extract_line

    #[test]
    fn test_binary_extract_line() {
        let buffer = b"OK\r\n";
        let mut index = 0;

        let output = binary_extract_line(buffer, &mut index).unwrap();

        assert_eq!(output, b"OK");
        assert_eq!(index, 4);
    }

    #[test]
    fn test_binary_extract_line_longer_string() {
        let buffer = b"ECHO\r\n";
        let mut index = 0;

        let output = binary_extract_line(buffer, &mut index).unwrap();

        assert_eq!(output, b"ECHO");
        assert_eq!(index, 6);
    }

    #[test]
    fn test_binary_extract_line_empty_buffer() {
        let buffer = b"";
        let mut index = 0;

        match binary_extract_line(buffer, &mut index) {
            Err(RespError::OutOfBounds(index)) => {
                assert_eq!(index, 0);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_binary_extract_line_no_separator() {
        let buffer = b"OK";
        let mut index = 0;

        match binary_extract_line(buffer, &mut index) {
            Err(RespError::OutOfBounds(index)) => {
                assert_eq!(index, 2);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_binary_extract_line_index_too_advanced() {
        let buffer = b"OK";
        let mut index = 1;

        match binary_extract_line(buffer, &mut index) {
            Err(RespError::OutOfBounds(index)) => {
                assert_eq!(index, 2);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_binary_extract_line_half_separator() {
        let buffer = b"OK\r";
        let mut index = 0;

        match binary_extract_line(buffer, &mut index) {
            Err(RespError::OutOfBounds(index)) => {
                assert_eq!(index, 3);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_binary_extract_line_incorrect_separator() {
        let buffer = b"OK\n";
        let mut index = 0;

        match binary_extract_line(buffer, &mut index) {
            Err(RespError::OutOfBounds(index)) => {
                assert_eq!(index, 3);
            }
            _ => panic!(),
        }
    }

    // binary_extract_line_as_string

    #[test]
    fn test_binary_extract_line_as_string() {
        let buffer = b"OK\r\n";
        let mut index = 0;

        let output = binary_extract_line_as_string(buffer, &mut index).unwrap();

        assert_eq!(output, "OK");
        assert_eq!(index, 4);
    }

    #[test]
    fn test_binary_extract_line_as_string_invalid_utf8() {
        let buffer = vec![0xFF, 0xFE, b'\r', b'\n'];
        let mut index = 0;

        let error = binary_extract_line_as_string(&buffer, &mut index).unwrap_err();

        assert_eq!(error, RespError::FromUtf8);
    }

    // resp_remove_type

    #[test]
    fn test_resp_remove_type() {
        let buffer = b"+OK\r\n";
        let mut index = 0;

        resp_remove_type('+', buffer, &mut index).unwrap();

        assert_eq!(index, 1);
    }

    #[test]
    fn test_resp_remove_type_error() {
        let buffer = b"*OK\r\n";
        let mut index = 0;

        let error = resp_remove_type('+', buffer, &mut index).unwrap_err();

        assert_eq!(index, 0);
        assert_eq!(error, RespError::WrongType);
    }

    // parse_simple_string

    #[test]
    fn test_parse_simple_string() {
        let buffer = b"+OK\r\n";
        let mut index = 0;

        let output = parse_simple_string(buffer, &mut index).unwrap();

        assert_eq!(output, Resp::simple_string("OK"));
        assert_eq!(index, 5);
    }

    // bytes_to_resp

    #[test]
    fn test_bytes_to_resp_simple_string() {
        let buffer = b"+OK\r\n";
        let mut index = 0;

        let output = bytes_to_resp(buffer, &mut index).unwrap();

        assert_eq!(output, Resp::simple_string("OK"));
        assert_eq!(index, 5);
    }

    #[test]
    fn test_bytes_to_resp_bulk_string() {
        let buffer = b"$2\r\nOK\r\n";
        let mut index = 0;

        let output = bytes_to_resp(buffer, &mut index).unwrap();

        assert_eq!(output, Resp::bulk_string("OK"));
        assert_eq!(index, 8);
    }

    #[test]
    fn test_bytes_to_resp_array() {
        let buffer = b"*2\r\n+OK\r\n$5\r\nVALUE\r\n";
        let mut index = 0;

        let output = bytes_to_resp(buffer, &mut index).unwrap();

        assert_eq!(
            output,
            Resp::Array(vec![Resp::simple_string("OK"), Resp::bulk_string("VALUE")]),
        );
        assert_eq!(index, 20);
    }

    #[test]
    fn test_bytes_to_resp_unknown() {
        let buffer = b"?OK\r\n";
        let mut index = 0;

        let error = bytes_to_resp(buffer, &mut index).unwrap_err();

        assert_eq!(error, RespError::Unknown);
        assert_eq!(index, 0);
    }

    // binary_extract_bytes

    #[test]
    fn test_binary_extract_bytes() {
        let buffer = b"SOMEBYTES";
        let mut index = 0;

        let output = binary_extract_bytes(buffer, &mut index, 6).unwrap();

        assert_eq!(output, b"SOMEBY");
        assert_eq!(index, 6);
    }

    #[test]
    fn test_binary_extract_bytes_out_of_bounds() {
        let buffer = b"SOMEBYTES";
        let mut index = 0;

        let error = binary_extract_bytes(buffer, &mut index, 10).unwrap_err();

        assert_eq!(error, RespError::OutOfBounds(9));
        assert_eq!(index, 0);
    }

    // parse_bulk_string

    #[test]
    fn test_parse_bulk_string() {
        let buffer = b"$2\r\nOK\r\n";
        let mut index = 0;

        let output = parse_bulk_string(buffer, &mut index).unwrap();

        assert_eq!(output, Resp::bulk_string("OK"));
        assert_eq!(index, 8);
    }

    #[test]
    fn test_parse_bulk_string_empty() {
        let buffer = b"$-1\r\n";
        let mut index = 0;

        let output = parse_bulk_string(buffer, &mut index).unwrap();

        assert_eq!(output, Resp::Null);
        assert_eq!(index, 5);
    }

    #[test]
    fn test_parse_bulk_string_unparsable_length() {
        let buffer = b"$wrong\r\nOK\r\n";
        let mut index = 0;

        let error = parse_bulk_string(buffer, &mut index).unwrap_err();

        assert_eq!(error, RespError::ParseInt);
        assert_eq!(index, 8);
    }

    #[test]
    fn test_parse_bulk_string_negative_length() {
        let buffer = b"$-7\r\nOK\r\n";
        let mut index = 0;

        let error = parse_bulk_string(buffer, &mut index).unwrap_err();

        assert_eq!(error, RespError::IncorrectLength(-7));
        assert_eq!(index, 5);
    }

    #[test]
    fn test_parse_bulk_string_data_too_short() {
        let buffer = b"$7\r\nOK\r\n";
        let mut index = 0;

        let error = parse_bulk_string(buffer, &mut index).unwrap_err();

        assert_eq!(error, RespError::OutOfBounds(8));
        assert_eq!(index, 4);
    }

    // parse_array

    #[test]
    fn test_parse_array() {
        let buffer = b"*2\r\n+OK\r\n$5\r\nVALUE\r\n";
        let mut index = 0;

        let output = parse_array(buffer, &mut index).unwrap();

        assert_eq!(
            output,
            Resp::Array(vec![Resp::simple_string("OK"), Resp::bulk_string("VALUE")]),
        );
        assert_eq!(index, 20);
    }

    #[test]
    fn test_parse_array_invalid_length() {
        let buffer = b"*-1\r\n+OK\r\n$5\r\nVALUE\r\n";
        let mut index = 0;

        let error = parse_array(buffer, &mut index).unwrap_err();

        assert_eq!(error, RespError::IncorrectLength(-1));
        assert_eq!(index, 5);
    }

    // bulk_string_from_vec

    #[test]
    fn test_bulk_string_from_vec() {
        let strings = vec![
            String::from("First string"),
            String::from("Second string"),
            String::from("Third string"),
        ];

        let bulk_string = Resp::bulk_string_from_vec(&strings);

        assert_eq!(
            bulk_string,
            Resp::bulk_string("First string\r\nSecond string\r\nThird string"),
        );
    }
}
