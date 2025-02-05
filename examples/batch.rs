use anyhow::Result;
use arrowdb::db::{Dist, hnsw::HNSW};
use arrowdb::store::fjall::FjallStore;
use rand::distributions::Uniform;
use rand::prelude::*;
use fjall::Config;

fn main() -> Result<()> {
    let space = Config::new("test").open_transactional().unwrap();
    let store = FjallStore::open(&space, "default");
    let hnsw = HNSW::new(store, 20, 200, 16, Dist::L2);
    let dim = 1024;

    let nb_elem = 1024;
    let mut rng = thread_rng();
    let unif = Uniform::<f32>::new(0., 1.);
    let mut data = Vec::with_capacity(nb_elem);
    for _ in 0..nb_elem {
        let column = (0..dim).map(|_| rng.sample(unif)).collect::<Vec<f32>>();
        data.push(column);
    }

    let dest = data[100].clone();
    hnsw.insert_batch(data)?;
    println!("{:?}", hnsw.search(dest, 10));
    Ok(())
}
