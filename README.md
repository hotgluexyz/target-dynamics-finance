# target-dynamics-finance

`target-dynamics-finance` is a Singer target for DynamicsFinance.

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install target-dynamics-finance
```

## Configuration

### Accepted Config Options

- [ ] `Developer TODO:` Provide a list of config options accepted by the target.

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-dynamics-finance --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

- [ ] `Developer TODO:` If your target requires special access on the source system, or any special authentication requirements, provide those here.

## Usage

You can easily run `target-dynamics-finance` by itself or in a pipeline

### Executing the Target Directly

```bash
target-dynamics-finance --version
target-dynamics-finance --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-dynamics-finance --config /path/to/target-dynamics-finance-config.json
```

## Developer Resources

- [ ] `Developer TODO:` As a first step, scan the entire project for the text "`TODO:`" and complete any recommended steps, deleting the "TODO" references once completed.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `target_dynamics_finance/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-dynamics-finance` CLI interface directly using `poetry run`:

```bash
poetry run target-dynamics-finance --help
```
