# Run the first time this project is instantiated

# uv could be installed as a tool, but we assume it is not installed so we do it from scratch

UV_VERSION=">=0.7,<0.8"
PYTHON_VERSION=3.11

# This part could be replaced by having uv as tool. I prefer to bootstrap all in the project
python3 -m venv .uv_venv
source .uv_venv/bin/activate
python3 -m pip install "uv${UV_VERSION}"

# Installing venv for the project
uv venv -p "${PYTHON_VERSION}" .venv
source .venv/bin/activate
uv sync