extern crate prost_build;

fn main() {
    std::env::set_var("PROTOC", protobuf_src::protoc());

    prost_build::compile_protos(&["./PulsarApi.proto"], &["./"]).unwrap();
}
