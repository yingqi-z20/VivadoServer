mod support;
use reqwest::{Method, StatusCode};
use std::{
    fs,
    io::{Cursor, Read},
    os::unix::fs::symlink,
};
use support::{TestServer, error_json, ok_json};

#[tokio::test]
async fn runtime_reads_are_authenticated_phase_independent_and_binary_preserving() {
    let server = TestServer::new().await;
    let project = server.temp.path().join("demo");
    fs::create_dir_all(project.join("inputs")).unwrap();
    fs::create_dir_all(project.join("build/empty")).unwrap();
    fs::write(project.join("inputs/top.v"), b"module top; endmodule\n").unwrap();
    let binary = [0, 159, 255, 10, 13, 128, 7, 42];
    fs::write(project.join("build/result.bit"), binary).unwrap();
    fs::write(project.join("build/run:1?.rpt"), "timing met\n").unwrap();
    let base = "/v1/projects/demo";
    assert_eq!(
        server
            .client
            .get(format!("{}{base}/workspace", server.base))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let listing = ok_json(
        server
            .request(Method::GET, &format!("{base}/workspace"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(listing["exists"], true);
    assert_eq!(listing["entries"].as_array().unwrap().len(), 2);
    assert_eq!(listing["entries"][0]["kind"], "dir");
    let preview = ok_json(
        server
            .request(Method::GET, &format!("{base}/workspace-preview"))
            .query(&[("path", "inputs/top.v")])
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(preview["kind"], "text");
    assert_eq!(preview["text"], "module top; endmodule\n");
    for (name, content) in [
        ("nul.bin", vec![0, 65]),
        ("invalid.bin", vec![0xff]),
        ("control.bin", vec![b'a', 0x1b]),
        ("c1.txt", "a\u{0085}b".as_bytes().to_vec()),
    ] {
        fs::write(project.join(name), content).unwrap();
        let preview = ok_json(
            server
                .request(Method::GET, &format!("{base}/workspace-preview"))
                .query(&[("path", name)])
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(preview["kind"], "binary", "{name}");
        assert!(preview.get("text").is_none());
    }
    fs::write(project.join("large.txt"), vec![b'a'; 1024 * 1024 + 1]).unwrap();
    fs::write(project.join("lines.txt"), "a\n".repeat(20000)).unwrap();
    for name in ["large.txt", "lines.txt"] {
        let preview = ok_json(
            server
                .request(Method::GET, &format!("{base}/workspace-preview"))
                .query(&[("path", name)])
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(preview["kind"], "too_large");
        assert!(preview.get("text").is_none());
    }
    let download = server
        .request(Method::GET, &format!("{base}/workspace-file"))
        .query(&[("path", "build/result.bit")])
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), StatusCode::OK);
    assert_eq!(download.headers()["x-workspace-snapshot"], "non-atomic");
    assert!(
        download.headers()["content-disposition"]
            .to_str()
            .unwrap()
            .contains("attachment")
    );
    assert_eq!(download.bytes().await.unwrap().as_ref(), binary);
    let unusual = server
        .request(Method::GET, &format!("{base}/workspace-file"))
        .query(&[("path", "build/run:1?.rpt")])
        .send()
        .await
        .unwrap();
    assert_eq!(unusual.bytes().await.unwrap().as_ref(), b"timing met\n");
    let missing = ok_json(
        server
            .request(Method::GET, "/v1/projects/missing/workspace")
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(missing["exists"], false);
    error_json(
        server
            .request(Method::GET, &format!("{base}/workspace-preview"))
            .query(&[("path", "missing")])
            .send()
            .await
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await;

    // Preview/list/download remain usable during an active Vivado workflow.
    let workflow = server.create("running", true).await;
    server.push(workflow, &[("generated.bin", &binary)]).await;
    server.start(workflow, &[]).await;
    assert_eq!(server.info(workflow).await["status"], "running");
    let response = server
        .request(Method::GET, "/v1/projects/running/workspace-file")
        .query(&[("path", "generated.bin")])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), binary);
    server.cancel(workflow).await;
    server.shutdown().await;
}

#[tokio::test]
async fn links_traversal_devices_and_metadata_never_escape_the_project() {
    let server = TestServer::new().await;
    let project = server.temp.path().join("demo");
    fs::create_dir_all(project.join(".vivado-server")).unwrap();
    fs::write(project.join(".vivado-server/secret"), "hidden").unwrap();
    fs::write(project.join("safe.txt"), "safe").unwrap();
    fs::write(project.join("line\nfeed"), "omitted").unwrap();
    let outside = tempfile::tempdir().unwrap();
    fs::write(outside.path().join("secret"), "never expose").unwrap();
    symlink(outside.path(), project.join("link")).unwrap();
    symlink(outside.path().join("secret"), project.join("secret-link")).unwrap();
    fs::hard_link(
        project.join(".vivado-server/secret"),
        project.join("hardlink"),
    )
    .unwrap();
    let _socket = std::os::unix::net::UnixListener::bind(project.join("socket")).unwrap();
    let fifo = std::ffi::CString::new(project.join("pipe").to_str().unwrap()).unwrap();
    assert_eq!(unsafe { nix::libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    for endpoint in [
        "workspace",
        "workspace-preview",
        "workspace-file",
        "workspace-archive",
    ] {
        for path in [
            "../secret",
            "/etc/passwd",
            "link/secret",
            "secret-link",
            ".vivado-server",
            ".vivado-server/secret",
            "a//b",
            "a/../safe.txt",
            "a\\b",
            "pipe",
            "socket",
            "hardlink",
        ] {
            let response = server
                .request(Method::GET, &format!("/v1/projects/demo/{endpoint}"))
                .query(&[("path", path)])
                .send()
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{endpoint} {path}: {}",
                response.text().await.unwrap()
            );
        }
    }
    let listing = ok_json(
        server
            .request(Method::GET, "/v1/projects/demo/workspace")
            .send()
            .await
            .unwrap(),
    )
    .await;
    let names: Vec<_> = listing["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["safe.txt"]);
    error_json(
        server
            .request(Method::GET, "/v1/projects/demo/workspace?unexpected=1")
            .send()
            .await
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    symlink(outside.path(), server.temp.path().join("linked-project")).unwrap();
    error_json(
        server
            .request(Method::GET, "/v1/projects/linked-project/workspace")
            .send()
            .await
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await;
    server.shutdown().await;
}

#[tokio::test]
async fn directory_zip_preserves_binary_skips_links_and_releases_temporary_file() {
    let server = TestServer::new().await;
    let project = server.temp.path().join("demo");
    fs::create_dir_all(project.join("nested/empty")).unwrap();
    fs::create_dir(project.join(".vivado-server")).unwrap();
    fs::write(project.join(".vivado-server/secret"), "hidden").unwrap();
    let bytes = [0, 255, 100, 127, 200];
    fs::write(project.join("nested/a.bit"), bytes).unwrap();
    symlink("/etc/passwd", project.join("unsafe")).unwrap();
    let before: Vec<_> = fs::read_dir(server.temp.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    let response = server
        .request(Method::GET, "/v1/projects/demo/workspace-archive")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    // The response is already consumed by the assertion only on failure.
    let data = response.bytes().await.unwrap();
    let mut zip = zip::ZipArchive::new(Cursor::new(data)).unwrap();
    assert_eq!(zip.len(), 3);
    assert!(zip.by_name("nested/empty/").unwrap().is_dir());
    let mut content = Vec::new();
    zip.by_name("nested/a.bit")
        .unwrap()
        .read_to_end(&mut content)
        .unwrap();
    assert_eq!(content, bytes);
    assert!(zip.by_name("unsafe").is_err());
    assert!(zip.by_name(".vivado-server/secret").is_err());
    let after: Vec<_> = fs::read_dir(server.temp.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(before, after);
    // A large regular file is still downloadable (HEAD need not read it),
    // while ZIP creation remains bounded and leaves no named temporary file.
    let large = fs::File::create(project.join("large.bin")).unwrap();
    large.set_len(3 * 1024 * 1024 * 1024).unwrap();
    let response = server
        .request(Method::HEAD, "/v1/projects/demo/workspace-file")
        .query(&[("path", "large.bin")])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-length"], "3221225472");
    error_json(
        server
            .request(Method::GET, "/v1/projects/demo/workspace-archive")
            .send()
            .await
            .unwrap(),
        StatusCode::PAYLOAD_TOO_LARGE,
    )
    .await;
    server.shutdown().await;
}

#[tokio::test]
async fn replacing_a_file_with_a_symlink_never_reads_the_link_target() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let server = TestServer::new().await;
    let project = server.temp.path().join("demo");
    fs::create_dir(&project).unwrap();
    let outside = tempfile::tempdir().unwrap();
    let secret = outside.path().join("secret");
    fs::write(&secret, "secret outside project").unwrap();
    fs::write(project.join("changing"), "safe").unwrap();
    let running = Arc::new(AtomicBool::new(true));
    let runner = running.clone();
    let writer = std::thread::spawn(move || {
        while runner.load(Ordering::Relaxed) {
            symlink(&secret, project.join("replacement")).unwrap();
            fs::rename(project.join("replacement"), project.join("changing")).unwrap();
            fs::write(project.join("replacement"), "safe").unwrap();
            fs::rename(project.join("replacement"), project.join("changing")).unwrap();
        }
    });
    for _ in 0..100 {
        let response = server
            .request(Method::GET, "/v1/projects/demo/workspace-file")
            .query(&[("path", "changing")])
            .send()
            .await
            .unwrap();
        if response.status() == StatusCode::OK {
            assert_eq!(response.bytes().await.unwrap().as_ref(), b"safe");
        } else {
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }
    running.store(false, Ordering::Relaxed);
    writer.join().unwrap();
    server.shutdown().await;
}
