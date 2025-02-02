use anyhow::Result;
use arrowdb::db::ArrowDB;
use rand::distributions::Uniform;
use rand::prelude::*;

fn main() -> Result<()> {
    let arrow_db = ArrowDB::new("arrow_db");
    let dim = 1024;
    arrow_db.create_collection("test1", 128).unwrap();
    let a_db = arrow_db.clone();
    a_db.create_collection("test2", dim).unwrap();

    println!("{:?}", arrow_db.get_collections());
    let nb_elem = 1024;
    let mut rng = thread_rng();
    let unif = Uniform::<f32>::new(0., 1.);
    let mut data = Vec::with_capacity(nb_elem);
        for _ in 0..nb_elem {
        let column = (0..dim).map(|_| rng.sample(unif)).collect::<Vec<f32>>();
        data.push(column);
    }

    let data_with_id = data.iter().zip(0..data.len()).collect::<Vec<_>>();  // give an id to each data

    let hns = arrow_db.get_hnsw("test2", dim)?;
    for _i in 0..data_with_id.len() {
        hns.insert(data_with_id[_i].0.clone());
    }
    println!("{:?}", hns.search(data_with_id[100].0.clone(), 10, 200));
    Ok(())
}
