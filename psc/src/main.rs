use chrono::prelude::*;
use reqwest::blocking::ClientBuilder;
use std::fs::{File, write};
use std::io::copy;
use std::path::Path;
use zip::ZipArchive;

const DATE_FORMAT: &str = "%Y-%m-%d";
const BASE_URL: &str = "https://download.companieshouse.gov.uk";

fn main() {
    let fname: String = define_partition_name("1of31");
    println!("[->] Generated filename: {}", fname);

    let zpath: &Path = download_zip_file(&fname);
    let tfile: File = extract_text_from_zip(zpath);
}

fn define_partition_name(partition: &str) -> String {
    let utc: String = Utc::now().format(DATE_FORMAT).to_string();
    let file_name: String = format!("psc-snapshot-{}_{}.zip", utc, partition);
    file_name
}

fn download_zip_file(zip_file: &str) -> &Path {
    let url: String = format!("{}/{}", BASE_URL, zip_file);
    println!("[->] Generated URL: {}", url);

    let client = ClientBuilder::new().user_agent("reqwest").build().unwrap();

    let zip_bytes = client.get(url).send().unwrap().bytes().unwrap();
    println!("[->] Downloaded {} bytes", zip_bytes.len());

    let path: &Path = Path::new(zip_file);
    write(&path, zip_bytes).unwrap();
    path
}

fn extract_text_from_zip(zip_path: &Path) -> File {
    // TODO understand why the program results in mutabiliy errors
    // when &tfile.name() is assigned to a variable

    println!("[->] Extracting zip path: {:?}", zip_path);

    let zfile = File::open(zip_path).unwrap();
    let mut archive = ZipArchive::new(zfile).unwrap();
    let mut tfile = archive.by_index(0).unwrap();
    let mut outfile = File::create(&tfile.name()).unwrap();

    copy(&mut tfile, &mut outfile).unwrap();

    println!("[->] Extracted file: {}", &tfile.name());
    outfile
}
