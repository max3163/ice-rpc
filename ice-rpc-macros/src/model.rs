//! The single reading of a `#[service]` trait.
//!
//! Everything the generators need about a service — names, versions, generated
//! type names, and one [`RpcMethod`] per method — is computed **once**, here, and
//! validated here.
//!
//! Before this module, `service()` walked the trait inline and
//! `nodejs_methods_vec` walked it a second time, each recomputing the request
//! variant name, the argument list and the return types; the service name was
//! validated by a block of three rules that `validate_channel_name` repeated
//! word for word for the group. A generator could therefore read a signature the
//! other passes had rejected, and a name could be accepted for one role and
//! refused for the other.

use proc_macro2::{Ident, Span};
use syn::{ItemTrait, LitInt, LitStr, TraitItem, Type, Visibility};

use crate::codegen::helpers::{extract_rpc_result_types, g_variant_name};
use crate::{METHOD_NAME_LEN, SERVICE_NAME_LEN};

/// Optional parameters of the `#[service]` macro.
///
/// - `#[service]` → the logical name = the trait name in lowercase.
/// - `#[service("MyService")]` → explicit logical name.
/// - `#[service(version = 1)]` → service interface version (default: `1`).
/// - `#[service(..., group = "db")]` → the **channel** this service shares with
///   the other services of the same group. A channel is the unit of transport:
///   it owns one request channel, one response channel and one dispatch thread,
///   and the samples are routed by the service id carried in the header.
///   Defaults to the service name, i.e. one channel per service.
/// - `#[service(..., max_slice_len = 4096)]` → the initial slice length of the
///   channel's publishers, in payload elements. It is a property of the
///   **channel**, so every service of a group must declare the same value; when
///   omitted, the runtime default (`DEFAULT_MAX_SLICE_LEN`, 256) applies.
/// - `#[service("MyService", version = 2, group = "db")]` → all.
#[derive(Debug)]
pub struct ServiceAttr {
    /// Explicit logical name, or `None` to derive it from the trait name.
    pub logical_name: Option<String>,
    /// Channel shared with the other services of the same group.
    pub group: Option<String>,
    /// Service interface version carried in the RPC header.
    pub service_version: u16,
    /// Initial slice length of the channel's publishers, when declared.
    pub max_slice_len: Option<usize>,
}

impl syn::parse::Parse for ServiceAttr {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut logical_name: Option<String> = None;
        let mut group: Option<String> = None;
        let mut service_version: u16 = 1;
        let mut max_slice_len: Option<usize> = None;

        if input.is_empty() {
            return Ok(Self {
                logical_name,
                group,
                service_version,
                max_slice_len,
            });
        }

        while !input.is_empty() {
            if input.peek(LitStr) {
                let name: LitStr = input.parse()?;
                logical_name = Some(name.value());
            } else {
                let ident: Ident = input.parse()?;
                if ident == "group" {
                    input.parse::<syn::Token![=]>()?;
                    let lit: LitStr = input.parse()?;
                    group = Some(lit.value());
                } else if ident == "version" {
                    input.parse::<syn::Token![=]>()?;
                    let lit: LitInt = input.parse()?;
                    service_version = lit.base10_parse::<u16>()?;
                } else if ident == "max_slice_len" {
                    input.parse::<syn::Token![=]>()?;
                    let lit: LitInt = input.parse()?;
                    let value = lit.base10_parse::<usize>()?;
                    // A slice of 0 elements cannot carry an RPC payload, and
                    // iceoryx2 refuses it: reject it here rather than at runtime.
                    if value == 0 {
                        return Err(syn::Error::new(
                            lit.span(),
                            "max_slice_len must be greater than 0",
                        ));
                    }
                    max_slice_len = Some(value);
                } else {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown parameter `{ident}` for #[service]"),
                    ));
                }
            }

            if input.is_empty() {
                break;
            }
            input.parse::<syn::Token![,]>()?;
        }

        Ok(Self {
            logical_name,
            group,
            service_version,
            max_slice_len,
        })
    }
}

/// Which name a validation error is about, so the message can hint at the right
/// parameter.
#[derive(Clone, Copy)]
enum NameKind {
    Service,
    Group,
}

impl NameKind {
    /// The parameter to suggest when the name is wrong.
    const fn parameter(self) -> &'static str {
        match self {
            NameKind::Service => "Use #[service(\"ShortName\")] to specify a shorter name.",
            NameKind::Group => "Use #[service(..., group = \"ShortName\")].",
        }
    }

    /// How the name is called in the messages.
    const fn label(self) -> &'static str {
        match self {
            NameKind::Service => "Service name",
            NameKind::Group => "Channel name",
        }
    }

    /// Length limit, shared with the wire framing.
    const fn max_len(self) -> usize {
        // A group becomes part of the iceoryx2 service names, so it obeys the
        // same limit as a service name.
        SERVICE_NAME_LEN
    }
}

/// Validates a service or channel name.
///
/// One implementation for both roles: they become parts of the same iceoryx2
/// service names, so they accept exactly the same characters.
fn validate_name(kind: NameKind, name: &str, span: Span) -> syn::Result<()> {
    let max = kind.max_len();
    if name.len() > max {
        return Err(syn::Error::new(
            span,
            format!(
                "{} '{name}' too long ({} > {max} characters). {}",
                kind.label(),
                name.len(),
                kind.parameter()
            ),
        ));
    }

    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(syn::Error::new(
            span,
            format!(
                "Invalid {} '{name}': only ASCII alphanumeric characters, '_' and '-' are allowed.",
                kind.label().to_lowercase()
            ),
        ));
    }

    if let Some(first) = name.chars().next() {
        if !first.is_ascii_alphanumeric() {
            return Err(syn::Error::new(
                span,
                format!(
                    "Invalid {} '{name}': must start with a letter or a digit.",
                    kind.label().to_lowercase()
                ),
            ));
        }
    }

    Ok(())
}

/// One RPC method of a service trait, read once.
///
/// No `Debug`: it holds `syn` nodes, which do not implement it without the
/// `extra-traits` feature of `syn`.
pub struct RpcMethod {
    /// Method name, as written in the trait.
    pub fn_name: Ident,
    /// Request-enum variant holding the arguments
    /// (`{Trait}Request::{Variant}`).
    pub var_name: Ident,
    /// Argument names, without the receiver.
    pub arg_names: Vec<Ident>,
    /// Argument types, in the same order as [`RpcMethod::arg_names`].
    pub arg_types: Vec<Type>,
    /// Declared return type (`Observable<T, E>`).
    pub output_type: Type,
    /// `T` of the declared return type.
    pub ok_type: Type,
    /// `E` of the declared return type.
    pub err_type: Type,
}

/// Everything the generators need about one `#[service]` trait.
pub struct ServiceModel {
    /// The annotated trait.
    pub trait_name: Ident,
    /// Visibility inherited from the trait; the generated types reuse it.
    pub visibility: Visibility,
    /// Logical service name (`#[service("Name")]`, or the trait name lowercased).
    pub logical_name: String,
    /// Channel shared by the services of the same group.
    pub group: String,
    /// Service interface version carried in the RPC header.
    pub service_version: u16,
    /// Initial slice length of the channel's publishers, when declared.
    pub max_slice_len: Option<usize>,
    /// `{Trait}Request`.
    pub req_enum_name: Ident,
    /// `{Trait}Client`.
    pub client_name: Ident,
    /// `{Trait}Server`.
    pub server_name: Ident,
    /// `{Trait}Proxy`.
    pub proxy_name: Ident,
    /// `{Trait}Mode`.
    pub mode_name: Ident,
    /// `__{Trait}ServiceInitDefault`.
    pub init_default_name: Ident,
    /// The methods, in declaration order, with their variant discriminants.
    pub methods: Vec<RpcMethod>,
}

impl ServiceModel {
    /// Reads and validates the annotated trait.
    ///
    /// This is the only place where the trait is walked: the generators receive
    /// this model or a narrow view built from it, and cannot disagree about what
    /// the trait declares.
    ///
    /// # Errors
    /// Returns a [`syn::Error`] spanned on the offending item: an invalid service
    /// or channel name, a method name over the wire limit, or a method whose
    /// signature is not `-> Observable<T, E>`.
    pub fn read(attr: &ServiceAttr, trait_item: &ItemTrait) -> syn::Result<Self> {
        let trait_name = trait_item.ident.clone();
        let visibility = trait_item.vis.clone();

        let logical_name = attr
            .logical_name
            .clone()
            .unwrap_or_else(|| trait_name.to_string().to_lowercase());
        validate_name(NameKind::Service, &logical_name, trait_name.span())?;

        let group = attr.group.clone().unwrap_or_else(|| logical_name.clone());
        validate_name(NameKind::Group, &group, trait_name.span())?;

        let methods = trait_item
            .items
            .iter()
            .filter_map(|item| match item {
                TraitItem::Fn(method) => Some(Self::read_method(method)),
                _ => None,
            })
            .collect::<syn::Result<Vec<_>>>()?;

        Ok(Self {
            trait_name: trait_name.clone(),
            visibility,
            logical_name,
            group,
            service_version: attr.service_version,
            max_slice_len: attr.max_slice_len,
            req_enum_name: Ident::new(&format!("{trait_name}Request"), trait_name.span()),
            client_name: Ident::new(&format!("{trait_name}Client"), trait_name.span()),
            server_name: Ident::new(&format!("{trait_name}Server"), trait_name.span()),
            proxy_name: Ident::new(&format!("{trait_name}Proxy"), trait_name.span()),
            mode_name: Ident::new(&format!("{trait_name}Mode"), trait_name.span()),
            init_default_name: Ident::new(
                &format!("__{trait_name}ServiceInitDefault"),
                trait_name.span(),
            ),
            methods,
        })
    }

    /// Reads one method: name, arguments and return types.
    fn read_method(method: &syn::TraitItemFn) -> syn::Result<RpcMethod> {
        let fn_name = method.sig.ident.clone();
        let fn_name_str = fn_name.to_string();

        if fn_name_str.len() > METHOD_NAME_LEN {
            return Err(syn::Error::new(
                fn_name.span(),
                format!(
                    "Method name '{fn_name_str}' too long ({} > {METHOD_NAME_LEN} characters). \
                     Rename the method so that it is at most {METHOD_NAME_LEN} characters long.",
                    fn_name_str.len(),
                ),
            ));
        }

        let mut arg_names = Vec::new();
        let mut arg_types = Vec::new();
        for arg in method.sig.inputs.iter().skip(1) {
            if let syn::FnArg::Typed(pat_type) = arg {
                if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                    arg_names.push(pat_ident.ident.clone());
                    arg_types.push((*pat_type.ty).clone());
                }
            }
        }

        let output_type = match &method.sig.output {
            syn::ReturnType::Type(_, ty) => (**ty).clone(),
            syn::ReturnType::Default => {
                return Err(syn::Error::new_spanned(
                    &method.sig,
                    "RPC methods must declare a return type, e.g. `-> Observable<T, E>`",
                ))
            }
        };
        let (ok_type, err_type) = extract_rpc_result_types(&output_type)?;

        Ok(RpcMethod {
            var_name: Ident::new(&g_variant_name(&fn_name_str), fn_name.span()),
            fn_name,
            arg_names,
            arg_types,
            output_type,
            ok_type: (*ok_type).clone(),
            err_type: (*err_type).clone(),
        })
    }

    /// The methods in declaration order, paired with their request-enum
    /// discriminant: the position in the trait is the discriminant, so the
    /// provider and the consumer agree without any explicit numbering.
    pub fn numbered_methods(&self) -> impl Iterator<Item = (u8, &RpcMethod)> {
        self.methods
            .iter()
            .enumerate()
            .map(|(index, method)| (index as u8, method))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn model(attr: proc_macro2::TokenStream, trait_item: proc_macro2::TokenStream) -> ServiceModel {
        let attr: ServiceAttr = syn::parse2(attr).expect("attribute should parse");
        let item: ItemTrait = syn::parse2(trait_item).expect("trait should parse");
        ServiceModel::read(&attr, &item).expect("model should be read")
    }

    #[test]
    fn the_logical_name_defaults_to_the_lowercased_trait_name() {
        let model = model(quote! {}, quote! { trait MyService { } });
        assert_eq!(model.logical_name, "myservice");
        // With no group, the service is alone in its channel.
        assert_eq!(model.group, "myservice");
    }

    #[test]
    fn the_generated_type_names_derive_from_the_trait() {
        let model = model(quote! { "Db" }, quote! { trait GetPerson { } });
        assert_eq!(model.req_enum_name.to_string(), "GetPersonRequest");
        assert_eq!(model.client_name.to_string(), "GetPersonClient");
        assert_eq!(model.server_name.to_string(), "GetPersonServer");
        assert_eq!(model.proxy_name.to_string(), "GetPersonProxy");
        assert_eq!(model.mode_name.to_string(), "GetPersonMode");
    }

    #[test]
    fn a_method_carries_its_variant_arguments_and_result_types() {
        let model = model(
            quote! {},
            quote! {
                trait Db {
                    async fn get_age(&self, name: String, retry: u8) -> Observable<i32, MyError>;
                }
            },
        );

        assert_eq!(model.methods.len(), 1);
        let method = &model.methods[0];
        assert_eq!(method.fn_name.to_string(), "get_age");
        assert_eq!(method.var_name.to_string(), "GetAge");
        // The receiver is not an argument of the request.
        let names: Vec<String> = method.arg_names.iter().map(|a| a.to_string()).collect();
        assert_eq!(names, vec!["name", "retry"]);
        assert_eq!(method.arg_types.len(), 2);
        assert_eq!(method.ok_type.to_token_stream_string(), "i32");
        assert_eq!(method.err_type.to_token_stream_string(), "MyError");
    }

    #[test]
    fn methods_are_numbered_from_zero_in_declaration_order() {
        let model = model(
            quote! {},
            quote! {
                trait Db {
                    async fn first(&self) -> Observable<i32, E>;
                    async fn second(&self) -> Observable<i32, E>;
                }
            },
        );
        let numbered: Vec<(u8, String)> = model
            .numbered_methods()
            .map(|(index, method)| (index, method.fn_name.to_string()))
            .collect();
        assert_eq!(
            numbered,
            vec![(0, "first".to_string()), (1, "second".to_string())]
        );
    }

    #[test]
    fn a_name_over_the_limit_is_refused_for_both_roles() {
        let long = "A".repeat(SERVICE_NAME_LEN + 1);

        let err = validate_name(NameKind::Service, &long, Span::call_site()).unwrap_err();
        assert!(err.to_string().contains("Service name"), "{err}");

        let err = validate_name(NameKind::Group, &long, Span::call_site()).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("Channel name"), "{message}");
        assert!(message.contains("group = "), "{message}");
    }

    #[test]
    fn an_invalid_character_is_refused_for_both_roles() {
        assert!(validate_name(NameKind::Service, "Bad.Name", Span::call_site()).is_err());
        assert!(validate_name(NameKind::Group, "Bad/Group", Span::call_site()).is_err());
        // A hyphen and an underscore stay legal on both sides.
        assert!(validate_name(NameKind::Service, "Good-Name_1", Span::call_site()).is_ok());
        assert!(validate_name(NameKind::Group, "Good-Name_1", Span::call_site()).is_ok());
    }

    #[test]
    fn a_name_that_does_not_start_alphanumerically_is_refused() {
        assert!(validate_name(NameKind::Service, "_Hidden", Span::call_site()).is_err());
        assert!(validate_name(NameKind::Group, "-dash", Span::call_site()).is_err());
    }

    #[test]
    fn a_method_without_a_return_type_is_refused() {
        let attr: ServiceAttr = syn::parse2(quote! {}).unwrap();
        let item: ItemTrait = syn::parse2(quote! {
            trait Db {
                async fn broken(&self);
            }
        })
        .unwrap();

        let err = match ServiceModel::read(&attr, &item) {
            Ok(_) => panic!("a method without a return type must be refused"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("must declare a return type"),
            "{err}"
        );
    }

    #[test]
    fn an_unknown_attribute_parameter_is_refused() {
        let err = syn::parse2::<ServiceAttr>(quote! { unknown = 1 }).unwrap_err();
        assert!(err.to_string().contains("unknown parameter"), "{err}");
    }

    /// Small helper so the assertions read like the type they check.
    trait TokenStreamString {
        fn to_token_stream_string(&self) -> String;
    }

    impl TokenStreamString for Type {
        fn to_token_stream_string(&self) -> String {
            quote!(#self).to_string().replace(' ', "")
        }
    }
}
