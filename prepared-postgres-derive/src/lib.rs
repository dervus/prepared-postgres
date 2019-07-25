extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::DeriveInput;

#[proc_macro_derive(FromRow)]
pub fn from_row_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).expect("Unable to parse derive input token stream");
    impl_from_row(&ast)
}

fn impl_from_row(ast: &DeriveInput) -> TokenStream {
    let name = ast.ident.clone();
    
    if let syn::Data::Struct(data_struct) = &ast.data {
        match &data_struct.fields {
            syn::Fields::Named(fields) => {
                let field_names = fields.named.iter().map(|f| f.ident.clone().unwrap());
                let field_indexes = 0..(fields.named.len());
                let output = quote! {
                    impl ::prepared_postgres::FromRow for #name {
                        fn from_row(__input_row__: &::postgres::Row) -> ::prepared_postgres::Result<Self> {
                            ::std::result::Result::Ok(Self {
                                #(
                                    #field_names: __input_row__.try_get(#field_indexes)?
                                ),*
                            })
                        }
                    }
                };
                output.into()
            },
            syn::Fields::Unnamed(fields) => {
                let field_indexes = 0..(fields.unnamed.len());
                let output = quote! {
                    impl ::prepared_postgres::FromRow for #name {
                        fn from_row(__input_row__: &::postgres::Row) -> ::prepared_postgres::Result<Self> {
                            ::std::result::Result::Ok(Self(
                                #(
                                    __input_row__.try_get(#field_indexes)?
                                ),*
                            ))
                        }
                    }
                };
                output.into()
            },
            syn::Fields::Unit => {
                panic!("Unable to derive FromRow for a unit struct");
            }
        }
    } else {
        panic!("FromRow derive macro currently supports only structs");
    }
}
