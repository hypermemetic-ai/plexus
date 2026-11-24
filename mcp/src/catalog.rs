//! Build MCP tools from the public wire contract of each reflected mount.
use plexus_ir::{Field, Ir, Returns, Shape, Ty};
use rmcp::model::Tool;
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;

#[derive(Clone, Debug)]
pub(crate) struct Entry {
    pub tool: Tool,
    pub address: String,
    pub streaming: bool,
    pub input: bool,
}

fn segment(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'))
}

// Bracketed context belongs to dispatch, not to a published tool name. A
// period inside a context value does not start a child mount.
pub(crate) fn public_mount(place: &str) -> Result<Option<String>, String> {
    let mut rest = place;
    let mut names = Vec::new();
    loop {
        let end = rest.find(['.', '[']).unwrap_or(rest.len());
        let name = &rest[..end];
        if !segment(name) {
            return Err(format!("invalid mount name in {place:?}"));
        }
        if name.starts_with('_') {
            return Ok(None);
        }
        names.push(name);
        rest = &rest[end..];
        if let Some(keys) = rest.strip_prefix('[') {
            let (keys, after) = keys.split_once(']').ok_or("unterminated mount context")?;
            let mut seen = BTreeSet::new();
            for pair in keys.split(',') {
                let (key, value) = pair.split_once('=').ok_or("invalid mount key")?;
                if !segment(key)
                    || !seen.insert(key)
                    || value.is_empty()
                    || value
                        .bytes()
                        .any(|b| b.is_ascii_control() || b"[],=".contains(&b))
                {
                    return Err("invalid mount context".into());
                }
            }
            rest = after;
        }
        if rest.is_empty() {
            return Ok(Some(names.join(".")));
        }
        rest = rest.strip_prefix('.').ok_or("invalid mount path")?;
    }
}

pub(crate) fn tools(place: &str, ir: &Ir) -> Result<Vec<Entry>, String> {
    let Some(mount) = public_mount(place)? else {
        return Ok(Vec::new());
    };
    let mut entries = Vec::new();
    for method in &ir.methods {
        // A reflected method is one segment. A malformed name must never
        // route into another activation, a context, or reflection.
        if !segment(&method.name) || method.name.starts_with('_') {
            continue;
        }
        let input = method.input.is_some();
        if input && method.params.iter().any(|p| p.name == "_input") {
            continue;
        }
        let name = format!("{mount}.{}", method.name);
        if name.len() > 128 {
            continue;
        }
        let mut builder = Schema {
            ir,
            needed: BTreeSet::new(),
        };
        let mut object = builder.record(&method.params)?;
        if let Some(item) = &method.input {
            object["properties"]["_input"] = json!({
                "type": "array", "items": builder.ty(item)?, "maxItems": super::MAX_ITEMS,
                "description": "Input stream items in transmission order."
            });
        }
        let definitions = builder.definitions()?;
        if !definitions.is_empty() {
            object["$defs"] = Value::Object(definitions);
        }
        let Value::Object(schema) = object else {
            unreachable!("record returns an object")
        };
        let tool = Tool::new_with_raw(
            name,
            (!method.doc.is_empty()).then(|| method.doc.clone().into()),
            schema,
        );
        entries.push(Entry {
            tool,
            address: format!("{place}.{}", method.name),
            streaming: matches!(method.returns, Returns::Stream(_)),
            input,
        });
    }
    Ok(entries)
}

struct Schema<'a> {
    ir: &'a Ir,
    needed: BTreeSet<String>,
}

impl Schema<'_> {
    fn ty(&mut self, ty: &Ty) -> Result<Value, String> {
        Ok(match ty {
            Ty::Prim("string") => json!({"type":"string"}),
            Ty::Prim("char") => json!({"type":"string","minLength":1,"maxLength":1}),
            Ty::Prim("bool") => json!({"type":"boolean"}),
            Ty::Prim("bytes") => json!({"type":"string","contentEncoding":"base64"}),
            Ty::Prim("json") => json!({}),
            Ty::Prim("f32" | "f64") => json!({"type":"number"}),
            Ty::Prim(p) => {
                macro_rules! bounds { ($t:ty) => { json!({"type":"integer","minimum":<$t>::MIN,"maximum":<$t>::MAX}) }; }
                match *p {
                    "u8" => bounds!(u8),
                    "u16" => bounds!(u16),
                    "u32" => bounds!(u32),
                    "u64" => bounds!(u64),
                    "s8" => bounds!(i8),
                    "s16" => bounds!(i16),
                    "s32" => bounds!(i32),
                    "s64" => bounds!(i64),
                    _ => return Err(format!("unknown IR primitive {p}")),
                }
            }
            Ty::List(item) => json!({"type":"array","items":self.ty(item)?}),
            Ty::Option(item) => json!({"anyOf":[self.ty(item)?,{"type":"null"}]}),
            Ty::Map(item) => json!({"type":"object","additionalProperties":self.ty(item)?}),
            Ty::Tuple(items) => {
                let prefix = items
                    .iter()
                    .map(|t| self.ty(t))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut tuple = json!({"type":"array","items":false,"minItems":items.len(),"maxItems":items.len()});
                if !prefix.is_empty() {
                    tuple["prefixItems"] = prefix.into();
                }
                tuple
            }
            Ty::Named(name) => {
                if !segment(name) {
                    return Err(format!("invalid named IR type {name:?}"));
                }
                self.needed.insert(name.clone());
                json!({"$ref":format!("#/$defs/{name}")})
            }
        })
    }

    fn record(&mut self, fields: &[Field]) -> Result<Value, String> {
        let mut props = Map::new();
        let mut required = Vec::new();
        for f in fields {
            // Native serde and the parameter decoder both use the IR name
            // verbatim. Only enum and variant cases honor Field::wire.
            let ty =
                f.ty.as_ref()
                    .ok_or_else(|| format!("missing type of field {}", f.name))?;
            let mut schema = self.ty(ty)?;
            if !f.doc.is_empty() {
                schema["description"] = json!(f.doc);
            }
            if let Some(default) = &f.default {
                schema["default"] = default.clone();
            } else if !matches!(underlying(self.ir, ty)?, Ty::Option(_)) {
                required.push(&f.name);
            }
            if f.deprecated.is_some() {
                schema["deprecated"] = json!(true);
            }
            if props.insert(f.name.clone(), schema).is_some() {
                return Err(format!("duplicate field {}", f.name));
            }
        }
        Ok(
            json!({"type":"object","properties":props,"required":required,"additionalProperties":false}),
        )
    }

    // Collect just the types reachable from this tool. Emit each named type
    // once, even if it (transitively) refers to itself.
    fn definitions(&mut self) -> Result<Map<String, Value>, String> {
        let mut defs = Map::new();
        while let Some(name) = self.needed.pop_first() {
            if defs.contains_key(&name) {
                continue;
            }
            let def = self
                .ir
                .types
                .iter()
                .find(|d| d.name == name)
                .ok_or_else(|| format!("unknown IR type {name}"))?;
            let mut schema = match &def.shape {
                Shape::Record(fields) => self.record(fields)?,
                Shape::Enum(cases) => {
                    json!({"type":"string","enum":cases.iter().map(wire_case).collect::<Vec<_>>()})
                }
                Shape::Variant(cases) => {
                    let mut alternatives = Vec::new();
                    for case in cases {
                        let key = wire_case(case);
                        let payload = match (&case.ty, &case.record) {
                            (Some(ty), None) => self.ty(ty)?,
                            (None, Some(fields)) => self.record(fields)?,
                            (None, None) => {
                                alternatives.push(json!({"const":key}));
                                continue;
                            }
                            _ => return Err(format!("ambiguous IR variant case {key}")),
                        };
                        alternatives.push(json!({"type":"object","properties":{(key):payload},"required":[key],"additionalProperties":false}));
                    }
                    if alternatives.is_empty() {
                        json!({"not":{}})
                    } else {
                        json!({"oneOf": alternatives})
                    }
                }
                Shape::Alias(ty, limits) => {
                    let base = self.ty(ty)?;
                    if limits.is_empty() {
                        base
                    } else {
                        // Preserve the primitive's original bounds alongside narrower limits.
                        let mut constrained = json!({"allOf":[base]});
                        for (kind, bound) in limits {
                            let keyword = match kind.as_str() {
                                "min" => "minimum",
                                "max" => "maximum",
                                "pattern" => "pattern",
                                "min-len" => length_keyword(self.ir, ty, true)?,
                                "max-len" => length_keyword(self.ir, ty, false)?,
                                _ => return Err(format!("unknown IR limit {kind}")),
                            };
                            constrained[keyword] = bound.clone();
                        }
                        constrained
                    }
                }
            };
            if !def.doc.is_empty() {
                schema["description"] = json!(def.doc);
            }
            defs.insert(name, schema);
        }
        Ok(defs)
    }
}

fn wire_case(f: &Field) -> &str {
    f.wire.as_deref().unwrap_or(&f.name)
}

fn underlying<'a>(ir: &'a Ir, mut ty: &'a Ty) -> Result<&'a Ty, String> {
    let mut visited = BTreeSet::new();
    while let Ty::Named(name) = ty {
        if !visited.insert(name) {
            return Err("circular IR alias".into());
        }
        let Some(def) = ir.types.iter().find(|d| d.name == *name) else {
            return Err(format!("unknown IR type {name}"));
        };
        let Shape::Alias(inner, _) = &def.shape else {
            break;
        };
        ty = inner;
    }
    Ok(ty)
}

fn length_keyword(ir: &Ir, ty: &Ty, min: bool) -> Result<&'static str, String> {
    match underlying(ir, ty)? {
        Ty::List(_) | Ty::Tuple(_) => Ok(if min { "minItems" } else { "maxItems" }),
        Ty::Map(_) => Ok(if min {
            "minProperties"
        } else {
            "maxProperties"
        }),
        Ty::Prim("string") => Ok(if min { "minLength" } else { "maxLength" }),
        // The native constraint counts decoded octets; JSON Schema length
        // would count base64 characters. Preserve the unit explicitly.
        Ty::Prim("bytes") => Ok(if min {
            "x-plexus-minBytes"
        } else {
            "x-plexus-maxBytes"
        }),
        _ => Err("length constraint on unsupported type".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn interface(types: Value, methods: Value) -> Ir {
        Ir::read(
            &json!({"ir":"plexus/0","activation":"kiln","types":types,"methods":methods})
                .to_string(),
        )
        .unwrap()
    }

    #[test]
    fn keyed_child_retains_address_and_exposes_only_wire_inputs() {
        let ir = interface(
            json!([]),
            json!([
                {"name":"heat","caller":"required","grant":"heat","params":[{"name":"loaded-by","type":"string"}],"input":"bytes","stream":"json"},
                {"name":"_ir","params":[]},
                {"name":"a.b","params":[]},
                {"name":"conflict","params":[{"name":"_input","type":"bool"}],"input":"u8"},
                {"name":"local","params":[{"name":"_input","type":"string"}]}
            ]),
        );
        let list = tools("studio[tenant=acme.example].kiln", &ir).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].tool.name, "studio.kiln.heat");
        assert_eq!(list[0].address, "studio[tenant=acme.example].kiln.heat");
        assert!(list[0].streaming && list[0].input);
        let schema = &list[0].tool.input_schema;
        assert_eq!(schema["required"], json!(["loaded-by"]));
        assert_eq!(
            schema["properties"]["_input"]["items"]["contentEncoding"],
            "base64"
        );
        assert_eq!(schema["properties"].as_object().unwrap().len(), 2);
        assert!(!list[1].input && !list[1].streaming);
        assert!(tools("studio._admin", &ir).unwrap().is_empty());
    }

    #[test]
    fn schemas_preserve_wire_cases_defaults_recursive_refs_and_alias_bounds() {
        let ir = interface(
            json!([
                {"name":"state","enum":[{"name":"in-review","wire":"In Review"}]},
                {"name":"priority","alias":"u8","limits":{"min":1,"max":5}},
                {"name":"maybe","alias":{"option":"string"}},
                {"name":"event","variant":[
                    {"name":"done","wire":"All done"},
                    {"name":"change","wire":"Changed","record":[{"name":"node","type":{"option":"event"}},{"name":"state","type":"state"}]}
                ]}
            ]),
            json!([{"name":"send","params":[
            {"name":"state","type":"state"},{"name":"note","type":"maybe"},{"name":"priority","type":"priority","default":1},
            {"name":"coords","type":{"tuple":["s16","u64"]}}
        ],"input":"event"}]),
        );
        let tool = tools("events", &ir).unwrap().remove(0).tool;
        let schema = tool.input_schema;
        assert_eq!(schema["$defs"]["state"]["enum"], json!(["In Review"]));
        assert_eq!(schema["$defs"]["priority"]["maximum"], 5);
        assert_eq!(schema["required"], json!(["state", "coords"]));
        assert_eq!(schema["properties"]["coords"]["maxItems"], 2);
        let cases = &schema["$defs"]["event"]["oneOf"];
        assert_eq!(cases[0]["const"], "All done");
        let changed = &cases[1]["properties"]["Changed"];
        assert_eq!(changed["required"], json!(["state"]));
        assert_eq!(
            changed["properties"]["node"]["anyOf"][0]["$ref"],
            "#/$defs/event"
        );
    }
}
