use std::env;

fn main() {
    if env::var("DEBUG").unwrap_or_default() == "true" {
        println!("cargo:rustc-cfg=feature=\"use_dotenv\"");
    }
}
