use abash_core::SandboxError;

pub(crate) fn execute(args: &[String]) -> Result<Vec<u8>, SandboxError> {
    let rendered = match args {
        [] => {
            return Err(SandboxError::InvalidRequest(
                "expr requires at least one argument".to_string(),
            ));
        }
        [value] => value.clone(),
        [left, op, right] => eval_binary(left, op, right)?,
        _ => {
            return Err(SandboxError::InvalidRequest(
                "expr currently supports one value or one binary operation".to_string(),
            ));
        }
    };
    Ok(format!("{rendered}\n").into_bytes())
}

fn eval_binary(left: &str, op: &str, right: &str) -> Result<String, SandboxError> {
    match op {
        "+" | "-" | "*" | "/" | "%" => {
            let lhs = parse_int(left, op)?;
            let rhs = parse_int(right, op)?;
            let value = match op {
                "+" => lhs + rhs,
                "-" => lhs - rhs,
                "*" => lhs * rhs,
                "/" => {
                    if rhs == 0 {
                        return Err(SandboxError::InvalidRequest(
                            "expr division by zero".to_string(),
                        ));
                    }
                    lhs / rhs
                }
                "%" => {
                    if rhs == 0 {
                        return Err(SandboxError::InvalidRequest(
                            "expr division by zero".to_string(),
                        ));
                    }
                    lhs % rhs
                }
                _ => unreachable!(),
            };
            Ok(value.to_string())
        }
        "=" | "!=" | "<" | "<=" | ">" | ">=" => Ok(bool_to_expr(compare(left, op, right))),
        _ => Err(SandboxError::InvalidRequest(format!(
            "expr operator is not supported: {op}"
        ))),
    }
}

fn parse_int(value: &str, op: &str) -> Result<i64, SandboxError> {
    value.parse::<i64>().map_err(|_| {
        SandboxError::InvalidRequest(format!(
            "expr operator {op} currently requires integer operands"
        ))
    })
}

fn compare(left: &str, op: &str, right: &str) -> bool {
    if let (Ok(lhs), Ok(rhs)) = (left.parse::<i64>(), right.parse::<i64>()) {
        return match op {
            "=" => lhs == rhs,
            "!=" => lhs != rhs,
            "<" => lhs < rhs,
            "<=" => lhs <= rhs,
            ">" => lhs > rhs,
            ">=" => lhs >= rhs,
            _ => unreachable!(),
        };
    }

    match op {
        "=" => left == right,
        "!=" => left != right,
        "<" => left < right,
        "<=" => left <= right,
        ">" => left > right,
        ">=" => left >= right,
        _ => unreachable!(),
    }
}

fn bool_to_expr(value: bool) -> String {
    if value {
        "1".to_string()
    } else {
        "0".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic_and_compare_work() {
        assert_eq!(
            String::from_utf8(execute(&["2".into(), "+".into(), "3".into()]).unwrap()).unwrap(),
            "5\n"
        );
        assert_eq!(
            String::from_utf8(execute(&["beta".into(), ">".into(), "alpha".into()]).unwrap())
                .unwrap(),
            "1\n"
        );
    }
}
