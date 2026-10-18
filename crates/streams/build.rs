//! Compiles the crate's protos against the descriptors of the sdk-core protos they import, so the
//! generated types reuse sdk-core's. The stream service's protos are compiled first, on their own,
//! and the crate's API protos refer to them as `crate::proto::service`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let core_descriptors = std::env::var("DEP_TEMPORALIO_PROTOS_DESCRIPTOR_PATH")?;
    println!("cargo:rerun-if-changed=protos");
    println!("cargo:rerun-if-env-changed=DEP_TEMPORALIO_PROTOS_DESCRIPTOR_PATH");
    let config = || {
        let mut c = prost_build::Config::new();
        c.enable_type_names()
            .protoc_arg(format!("--descriptor_set_in={core_descriptors}"))
            .extern_path(
                ".temporal.api.common",
                "::temporalio_protos::temporal::api::common",
            )
            .extern_path(
                ".temporal.api.failure",
                "::temporalio_protos::temporal::api::failure",
            );
        c
    };
    config().compile_protos(
        &["protos/api/temporal/api/nexusservices/streamservice/v1/request_response.proto"],
        &["protos/api"],
    )?;
    config()
        .extern_path(
            ".temporal.api.nexusservices.streamservice.v1",
            "crate::proto::service",
        )
        .compile_protos(
            &["protos/temporal/sdk/streams/v1/streams.proto"],
            &["protos/api", "protos"],
        )?;
    Ok(())
}
