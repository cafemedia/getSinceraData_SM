# getSinceraData

This repository contains a simple script to fetch the Sincera ecosystem data. The API key is provided via the `SINCERA_API_KEY` GitHub secret. When run, the script stores the result in `output/ecosystem/ecosystem.json`.
If `AWS_BUCKET_NAME` is set, the entire `output/` directory is synced to the same folder structure in the bucket using `scripts/sync_output_to_s3.sh`.

## Usage

Ensure that the `SINCERA_API_KEY` environment variable is available (for example via GitHub Actions secrets). To upload the file to S3 you must also configure the AWS role via `AWS_ROLE_TO_ASSUME` and provide the bucket name in `AWS_BUCKET_NAME`.

```bash
./scripts/fetch_sincera_data.sh
```

## Reference sellers lists
On every merge into `main`, a GitHub Actions workflow downloads the latest `sellers.json` files from SheMedia, CafeMedia, Mediavine, Freestar, and Aditude. The files are committed to the `reference_sellers_lists/` directory.

### Sampling publisher A2CR

The `sample_a2cr.py` script reads every `sellers.json` file stored in
`reference_sellers_lists`, takes random samples from the domains listed,
  fetches A2CR data for each domain from OpenSincera and writes the
  raw results to the `output/raw_ac2r/` directory. The summary statistics are
  written to `output/ac2r_analysis/`. The
  script requires the `SINCERA_API_KEY` environment variable and Python
packages `requests` and `numpy`.
  When `AWS_BUCKET_NAME` is set, the entire `output/` directory is synced to the bucket
  using `scripts/sync_output_to_s3.sh` so the files appear under `raw_ac2r/` and
  `ac2r_analysis/`.

```bash
SINCERA_API_KEY=your_token SAMPLE_SIZE=5 python scripts/sample_a2cr.py
```
The optional `SAMPLE_SIZE` variable controls how many domains are sampled from each
`sellers.json` file. The default is 100.
