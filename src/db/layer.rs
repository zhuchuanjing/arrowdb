//得到概率层次的随机发生器
use rand::distributions::Uniform;
use rand::prelude::*;
pub(crate) struct LayerGenerator {
    rng: rand::rngs::StdRng,
    unif: Uniform<f64>,
    pub scale: f64,
    max_level: usize,
}

impl LayerGenerator {
    pub fn new(max_nb_connection: usize, max_level: usize) -> Self {
        let scale = 1. / (max_nb_connection as f64).ln();
        Self { rng: StdRng::from_entropy(), unif: Uniform::<f64>::new(0., 1.), scale, max_level }
    }

    pub fn generate(&mut self) -> usize {
        let level = -self.rng.sample(self.unif).ln() * self.scale;
        let mut ulevel = level.floor() as usize;
        if ulevel >= self.max_level {
            ulevel = self.rng.sample(Uniform::<usize>::new(0, self.max_level));
        }
        ulevel
    }
}
