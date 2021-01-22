use std::env::var;
use std::path::PathBuf;
use tempdir::TempDir;

mod functions;
use functions::{copy_files, find_proto_files, generate_pulsar_lib, get_commitish};

mod constants;
use constants::{CUSTOM_FIELD_ATTRIBUTES, CUSTOM_TYPE_ATTRIBUTES, PULSAR_COMMITISH, PULSAR_REPO};

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let pulsar_lib_target = root.join("../proto/src/pulsar.rs");
    let target_dir = root.join("../proto/src/prost");
    let out_dir = var("OUT_DIR")
        .map(PathBuf::from)
        .or_else(|_| TempDir::new("pulsar_proto_out").map(|d| d.into_path()))
        .unwrap();
    let pulsar_dir = var("PULSAR_DIR").unwrap_or_else(|_| "target/pulsar".to_string());

    println!(
        "[info] => Fetching {} at {} into {}",
        PULSAR_REPO, PULSAR_COMMITISH, pulsar_dir
    );
    get_commitish(&PathBuf::from(&pulsar_dir), PULSAR_REPO, PULSAR_COMMITISH); // This panics if it fails.

    let proto_paths = [
        format!("{}/pulsar-common/src/main/proto", pulsar_dir),
        format!("{}/pulsar-functions/proto/src/main/proto", pulsar_dir),
    ];
    let proto_includes_paths = [format!("{}", pulsar_dir)];
    // List available proto files
    let protos = find_proto_files(proto_paths.to_vec());
    // List available paths for dependencies
    let includes: Vec<PathBuf> = proto_includes_paths.iter().map(PathBuf::from).collect();

    // Compile proto files with added annotations, exchange prost_types to our own
    let mut pb = prost_build::Config::new();
    pb.out_dir(&out_dir);
    for type_attribute in CUSTOM_TYPE_ATTRIBUTES {
        pb.type_attribute(type_attribute.0, type_attribute.1);
    }
    for field_attribute in CUSTOM_FIELD_ATTRIBUTES {
        pb.field_attribute(field_attribute.0, field_attribute.1);
    }
    pb.compile_well_known_types();
    println!("[info] => Creating structs.");
    pb.compile_protos(&protos, &includes).unwrap();

    println!("[info] => Removing old structs and copying new structs.");
    copy_files(&out_dir, &target_dir); // This panics if it fails.
    generate_pulsar_lib(&out_dir, &pulsar_lib_target);

    println!("[info] => Done!");
}
