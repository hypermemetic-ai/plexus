//! Operator grant declarations. The application supplies the principal, method
//! and target types, and decides where a resolved grant applies.
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use std::{collections::HashMap, env, fs, io, path::Path};
use syn::{
    Ident, LitStr, Token, braced, parenthesized,
    parse::{ParseStream, Parser},
};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Grant<P, M, T> {
    pub principal: P,
    pub method: M,
    pub target: T,
}

impl<P: DeserializeOwned, M: DeserializeOwned, T: DeserializeOwned> Grant<P, M, T> {
    /// An unset variable selects the supplied defaults. A configured file
    /// replaces them completely, including when the file is empty.
    pub fn load_file_from_env(var: &str, defaults: Option<Vec<Self>>) -> io::Result<Vec<Self>> {
        let Some(path) = env::var_os(var) else {
            return Ok(defaults.unwrap_or_default());
        };
        let path = Path::new(&path);
        let read = || Self::parse(&fs::read_to_string(path)?);
        read().map_err(|e| io::Error::new(e.kind(), format!("{var} ({}): {e}", path.display())))
    }

    pub fn parse(text: &str) -> io::Result<Vec<Self>> {
        let parse = |input: ParseStream| {
            let mut aliases = HashMap::new();
            let mut grants = Vec::new();
            let mut entry = 0;
            while !input.is_empty() {
                entry += 1;
                let mut declaration = || -> syn::Result<()> {
                    let kind: Ident = input.parse()?;
                    let args;
                    parenthesized!(args in input);
                    match kind.to_string().as_str() {
                        "principal" => {
                            let name: Ident = args.parse()?;
                            args.parse::<Token![=]>()?;
                            let id = args.parse::<LitStr>()?.value();
                            serde_json::from_value::<P>(Value::String(id.clone()))
                                .map_err(|e| args.error(e))?;
                            if aliases.insert(name.to_string(), id).is_some() {
                                return Err(args.error(format!("duplicate principal {name}")));
                            }
                        }
                        "grant" => {
                            let fields = fields(&args, true, &aliases)?;
                            grants.push(
                                serde_json::from_value(Value::Object(fields))
                                    .map_err(|e| args.error(e))?,
                            );
                        }
                        _ => return Err(input.error(format!("unknown declaration {kind}"))),
                    }
                    if !args.is_empty() {
                        return Err(args.error("unexpected argument"));
                    }
                    if input.peek(Token![;]) {
                        input.parse::<Token![;]>()?;
                    }
                    Ok(())
                };
                declaration().map_err(|e| input.error(format!("entry {entry}: {e}")))?;
            }
            Ok(grants)
        };
        parse
            .parse_str(text)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }
}

fn fields(
    input: ParseStream,
    top: bool,
    aliases: &HashMap<String, String>,
) -> syn::Result<Map<String, Value>> {
    let mut out = Map::new();
    while !input.is_empty() {
        let key = input.parse::<Ident>()?.to_string();
        if top {
            input.parse::<Token![=]>()?;
        } else {
            input.parse::<Token![:]>()?;
        }
        let value = match (top, key.as_str()) {
            (true, "method") => {
                let mut method = input.parse::<Ident>()?.to_string();
                while input.peek(Token![.]) {
                    input.parse::<Token![.]>()?;
                    method.push('.');
                    method.push_str(&input.parse::<Ident>()?.to_string());
                }
                Value::String(method)
            }
            (true, "target") => {
                let inner;
                braced!(inner in input);
                Value::Object(fields(&inner, false, aliases)?)
            }
            (true, "principal") | (false, _) => {
                let text = if input.peek(LitStr) {
                    input.parse::<LitStr>()?.value()
                } else {
                    let name: Ident = input.parse()?;
                    aliases
                        .get(&name.to_string())
                        .cloned()
                        .ok_or_else(|| input.error(format!("unknown principal {name}")))?
                };
                Value::String(text)
            }
            _ => return Err(input.error(format!("unknown grant field {key}"))),
        };
        if out.insert(key.clone(), value).is_some() {
            return Err(input.error(format!("duplicate {key}")));
        }
        if !input.is_empty() {
            input.parse::<Token![,]>()?;
        }
    }
    Ok(out)
}
