//! Helpers for manipulating names and types for code generation.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{GenericArgument, PathArguments, Type};

/// Prefixes every top-level item of `tokens` with `#[allow(missing_docs)]`.
///
/// The generated surface mechanically mirrors the user's `#[service]` trait, so
/// a consumer crate that opts into the `missing_docs` lint would receive dozens
/// of warnings on items it can neither rename nor document.
///
/// The user's trait is deliberately *not* routed through this helper.
pub(crate) fn allow_missing_docs(tokens: TokenStream) -> TokenStream {
    let file: syn::File = match syn::parse2(tokens.clone()) {
        Ok(file) => file,
        // If parsing ever failed, emitting the tokens unchanged is better.
        Err(_) => return tokens,
    };
    let mut out = TokenStream::new();
    for item in file.items {
        out.extend(quote! {
            #[allow(missing_docs)]
            #item
        });
    }
    out
}

/// Converts a `snake_case` method name into `PascalCase` for the enum variants.
///
/// # Examples
/// ```ignore
/// assert_eq!(g_variant_name("get_user_name"), "GetUserName");
/// ```
pub(crate) fn g_variant_name(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = true;
    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }
    result
}

/// Extracts the `(OkType, ErrType)` types from the return type of an RPC method.
///
/// Supports the forms:
/// - `Observable<T, E>`
/// - `Result<Observable<T, E>, _>`
/// - `Result<Receiver<Event<T, E>>, _>`
///
/// # Returns
/// Tuple `(OkType, ErrType)` as `Box<Type>`.
///
/// # Errors
/// Returns a [`syn::Error`] spanned on the offending type when the shape is not
/// one of the supported ones. The caller turns it into a `compile_error!`
/// pointing at the method, instead of panicking inside the macro.
pub fn extract_rpc_result_types(ty: &Type) -> syn::Result<(Box<Type>, Box<Type>)> {
    if let Some((t, e)) = extract_two_generic_args(ty) {
        return Ok((Box::new(t.clone()), Box::new(e.clone())));
    }

    let ok_type = extract_first_generic_arg(ty).ok_or_else(|| {
        syn::Error::new_spanned(
            ty,
            "invalid RPC return type: expected `Observable<T, E>`, \
             `Result<Observable<T, E>, _>` or `Result<Event<T, E>, _>`",
        )
    })?;

    if let Some((t, e)) = extract_two_generic_args(ok_type) {
        return Ok((Box::new(t.clone()), Box::new(e.clone())));
    }

    let rpc_event = extract_first_generic_arg(ok_type).ok_or_else(|| {
        syn::Error::new_spanned(
            ty,
            "invalid RPC return type: the success type must be `Observable<T, E>` \
             or `Event<T, E>`",
        )
    })?;

    let (t_type, e_type) = extract_two_generic_args(rpc_event).ok_or_else(|| {
        syn::Error::new_spanned(
            rpc_event,
            "the event type must have exactly two generic parameters: `<T, E>`",
        )
    })?;

    Ok((Box::new(t_type.clone()), Box::new(e_type.clone())))
}

/// Extracts the first generic argument of a path type.
///
/// # Returns
/// * `Some(&Type)` — The first generic argument.
/// * `None` if the type has no generics.
pub fn extract_first_generic_arg(ty: &Type) -> Option<&Type> {
    if let Type::Path(type_path) = ty {
        let last_seg = type_path.path.segments.last()?;
        if let PathArguments::AngleBracketed(ref args) = last_seg.arguments {
            if let Some(GenericArgument::Type(inner)) = args.args.first() {
                return Some(inner);
            }
        }
    }
    None
}

/// Extracts the first two generic arguments of a path type.
///
/// # Returns
/// * `Some((&Type, &Type))` — The first two arguments.
/// * `None` if the type has fewer than 2 generics.
pub fn extract_two_generic_args(ty: &Type) -> Option<(&Type, &Type)> {
    if let Type::Path(type_path) = ty {
        let last_seg = type_path.path.segments.last()?;
        if let PathArguments::AngleBracketed(ref args) = last_seg.arguments {
            let mut iter = args.args.iter();
            let first = match iter.next()? {
                GenericArgument::Type(t) => t,
                _ => return None,
            };
            let second = match iter.next()? {
                GenericArgument::Type(t) => t,
                _ => return None,
            };
            return Some((first, second));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn variant_name_simple() {
        assert_eq!(g_variant_name("hello"), "Hello");
    }

    #[test]
    fn variant_name_snake_case() {
        assert_eq!(g_variant_name("get_user_name"), "GetUserName");
    }

    #[test]
    fn variant_name_single_word() {
        assert_eq!(g_variant_name("name"), "Name");
    }

    #[test]
    fn variant_name_already_pascal() {
        assert_eq!(g_variant_name("UserName"), "UserName");
    }

    #[test]
    fn variant_name_empty() {
        assert_eq!(g_variant_name(""), "");
    }

    #[test]
    fn variant_name_multiple_underscores() {
        assert_eq!(g_variant_name("get__user___name"), "GetUserName");
    }

    #[test]
    fn variant_name_trailing_underscore() {
        assert_eq!(g_variant_name("get_name_"), "GetName");
    }

    #[test]
    fn variant_name_leading_underscore() {
        assert_eq!(g_variant_name("_private"), "Private");
    }

    #[test]
    fn extract_first_generic_arg_from_option() {
        let ty: syn::Type = parse_quote! { Option<String> };
        let arg = extract_first_generic_arg(&ty);
        assert!(arg.is_some());
    }

    #[test]
    fn extract_first_generic_arg_no_generics() {
        let ty: syn::Type = parse_quote! { String };
        let arg = extract_first_generic_arg(&ty);
        assert!(arg.is_none());
    }

    #[test]
    fn extract_two_generic_args_from_result() {
        let ty: syn::Type = parse_quote! { Result<i32, String> };
        let (first, second) = extract_two_generic_args(&ty).unwrap();
        let first_str = quote::quote! { #first }.to_string();
        let second_str = quote::quote! { #second }.to_string();
        assert_eq!(first_str, "i32");
        assert_eq!(second_str, "String");
    }

    #[test]
    fn extract_two_generic_args_single_param() {
        let ty: syn::Type = parse_quote! { Vec<u8> };
        assert!(extract_two_generic_args(&ty).is_none());
    }

    #[test]
    fn extract_two_generic_args_no_params() {
        let ty: syn::Type = parse_quote! { String };
        assert!(extract_two_generic_args(&ty).is_none());
    }

    #[test]
    fn extract_rpc_types_from_observable() {
        let ty: syn::Type = parse_quote! { crate::Observable<i32, String> };
        let (ok_type, err_type) = extract_rpc_result_types(&ty).unwrap();
        let ok_str = quote::quote! { #ok_type }.to_string();
        let err_str = quote::quote! { #err_type }.to_string();
        assert_eq!(ok_str, "i32");
        assert_eq!(err_str, "String");
    }

    #[test]
    fn extract_rpc_types_from_result_with_two_args() {
        let ty: syn::Type = parse_quote! { Result<i32, String> };
        let (first, second) = extract_rpc_result_types(&ty).unwrap();
        let first_str = quote::quote! { #first }.to_string();
        let second_str = quote::quote! { #second }.to_string();
        assert_eq!(first_str, "i32");
        assert_eq!(second_str, "String");
    }
}
