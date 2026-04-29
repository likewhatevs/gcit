# rust-script recipes for gcit

# Print mutation score from last run
[script("rust-script")]
mutants-score:
    //! ```cargo
    //! [dependencies]
    //! serde = { version = "1", features = ["derive"] }
    //! serde_json = "1"
    //! ```
    #[derive(serde::Deserialize)]
    struct Lab { caught: usize, missed: usize, timeout: usize }
    fn main() {
        let lab: Lab = serde_json::from_str(
            &std::fs::read_to_string("mutants.out/outcomes.json")
                .expect("no mutants.out/outcomes.json — run 'just mutants' first"),
        ).unwrap();
        let denom = lab.caught + lab.missed + lab.timeout;
        if denom == 0 { println!("0 mutants generated"); return; }
        let score = lab.caught * 100 / denom;
        println!("Mutation score: {score}% ({} caught, {} missed, {} timeout)",
                 lab.caught, lab.missed, lab.timeout);
    }

# Render badge + push to gh-pages (requires GITHUB_TOKEN or GH_TOKEN)
[script("rust-script")]
publish-mutants:
    //! ```cargo
    //! [dependencies]
    //! serde = { version = "1", features = ["derive"] }
    //! serde_json = "1"
    //! ureq = { version = "2", features = ["json"] }
    //! base64 = "0.22"
    //! ```
    #[derive(serde::Deserialize)]
    struct Lab { caught: usize, missed: usize, timeout: usize }
    #[derive(serde::Serialize)]
    struct Badge { #[serde(rename = "schemaVersion")] schema: u8, label: &'static str, message: String, color: &'static str }

    fn main() {
        let lab: Lab = serde_json::from_str(
            &std::fs::read_to_string("mutants.out/outcomes.json")
                .expect("run 'just mutants' first"),
        ).unwrap();
        let denom = lab.caught + lab.missed + lab.timeout;
        let (msg, color) = if denom == 0 { ("0 mutants".into(), "lightgrey") } else {
            let s = lab.caught * 100 / denom;
            (format!("{s}%"), match s { 95.. => "brightgreen", 85.. => "green", 70.. => "yellow", 50.. => "orange", _ => "red" })
        };
        println!("Mutation score: {msg} ({} caught, {} missed, {} timeout)", lab.caught, lab.missed, lab.timeout);

        let badge = Badge { schema: 1, label: "mutants", message: msg, color };
        let json = serde_json::to_vec_pretty(&badge).unwrap();
        std::fs::write("mutants.out/badge.json", &json).unwrap();

        let token = std::env::var("GITHUB_TOKEN")
            .or_else(|_| std::env::var("GH_TOKEN"))
            .expect("set GITHUB_TOKEN or GH_TOKEN");
        let (owner, repo) = ("likewhatevs", "gcit");
        let url = format!("https://api.github.com/repos/{owner}/{repo}/contents/mutants.json");
        let auth = format!("Bearer {token}");

        let sha: Option<String> = ureq::get(&url)
            .query("ref", "gh-pages")
            .set("Authorization", &auth)
            .set("User-Agent", "gcit")
            .call().ok()
            .and_then(|r| r.into_json::<serde_json::Value>().ok())
            .and_then(|v| v["sha"].as_str().map(String::from));

        use base64::Engine;
        let mut body = serde_json::json!({
            "message": "refresh mutants badge",
            "branch": "gh-pages",
            "content": base64::engine::general_purpose::STANDARD.encode(&json),
        });
        if let Some(s) = sha { body["sha"] = s.into(); }

        ureq::put(&url)
            .set("Authorization", &auth)
            .set("User-Agent", "gcit")
            .send_json(body)
            .expect("upload failed");
        println!("published to gh-pages");
    }
