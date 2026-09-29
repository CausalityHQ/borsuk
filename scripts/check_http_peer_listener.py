"""Compile and test the actual example listener guard using only Rust std."""
from pathlib import Path
import subprocess
import tempfile

source = Path('crates/borsuk/examples/two_bit_http.rs').read_text()
start = source.index('    let listen: SocketAddr =')
end = source.index('    let authority =', start)
guard = source[start:end]
body = '''use std::{net::SocketAddr, error::Error};
fn validate(value: &str) -> Result<(), Box<dyn Error>> {
let args = ["", "", "", "", "", "", "", value];
''' + guard + '''Ok(())
}
#[test] fn listener_boundary() {
 for address in ["127.0.0.1:8080", "[::1]:8080", "10.0.0.1:8080", "172.16.0.1:8080", "192.168.1.1:8080"] {
  assert!(validate(address).is_ok(), "must permit {}", address);
 }
 for address in ["0.0.0.0:8080", "8.8.8.8:8080", "[::]:8080", "[2001:db8::1]:8080", "[::ffff:8.8.8.8]:8080"] {
  assert!(validate(address).is_err(), "must reject {}", address);
 }
}
'''
with tempfile.TemporaryDirectory() as directory:
    path = Path(directory)
    (path / 'guard.rs').write_text(body)
    subprocess.run(['rustc', '--edition=2021', '--test', str(path / 'guard.rs'), '-o', str(path / 'guard')], check=True)
    subprocess.run([str(path / 'guard')], check=True)
