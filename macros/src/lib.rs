use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemFn, parse::Parse, parse_macro_input};

struct MainArgs {
    thread_count: usize,
}

#[proc_macro_attribute]
pub fn test(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    if input_fn.sig.asyncness.is_none() {
        panic!("test function must be async");
    }

    let fn_block = input_fn.block;
    let fn_name = input_fn.sig.ident;
    let num_threads = get_thread_count(attr);
    proc_macro::TokenStream::from(quote! {
        #[test]
        fn #fn_name() {
            EventLoop::new(#num_threads).block_on(async #fn_block );
        }
    })
}

#[proc_macro_attribute]
pub fn main(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    if input_fn.sig.ident != "main" {
        panic!("async_runtime::main, should only be used on the 'main' function");
    }

    if input_fn.sig.asyncness.is_none() {
        panic!("main function must be async");
    }

    let fn_block = input_fn.block;
    let num_threads = get_thread_count(attr);
    proc_macro::TokenStream::from(quote! {
        fn main() {
            EventLoop::new(#num_threads).block_on(async #fn_block );
        }
    })
}

fn get_thread_count(attr: proc_macro::TokenStream) -> usize {
    let args = syn::parse2::<MainArgs>(TokenStream::from(attr))
        .expect("first argument must be 'thread_count = {amount}'");
    let num_threads = args.thread_count;
    if num_threads == 0 {
        panic!("thread_count must be bigger than 0");
    }

    num_threads
}

impl Parse for MainArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let thread_count = match input.parse::<syn::Ident>() {
            Ok(ident) => ident.to_string(),
            Err(_) => return Ok(MainArgs { thread_count: 1 }),
        };
        if thread_count != "thread_count" {
            return Err(input.error(""));
        }

        input.parse::<syn::Token![=]>()?;

        let thread_count = input.parse::<syn::LitInt>()?.base10_parse::<usize>()?;

        Ok(MainArgs { thread_count })
    }
}
