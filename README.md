# README
All code is in `src/` dir.

To run, `cd` to `src/` and run `./run.sh`. To stop execution, run `./stop.sh` (from another console, since the first one will be logging).

Compressed files downloaded from [Kaggle](https://www.kaggle.com/pablodroca/yelp-review-analysis) should be kept in `data/`.

To access to RabbitMQ web interface, go to [http://localhost:15672/#/](http://localhost:15672/#/), and sign in with credentials:
- user: guest
- pass: guest

# Node multiplicity
In the `docker-compose-dev.yaml`, the `--scale` parameter is used for `raw_data_receiver` and `histogram` nodes. Note that these numbers must coincide with the numbers of the env. vars. `NUM_OF_DATA_RECEIVERS` and `NUM_OF_HISTOGRAMMERS`.

# General Data
- Informe may be accessed [here](informe.pdf).
- Enunciado is available [here](enunciado.pdf).
