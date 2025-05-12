#!/bin/bash

# Lambda GPU Instance Setup Script for LLM Processing
# This script installs all necessary dependencies for running LLMs
# on a Lambda Labs GPU instance

set -e  # Exit on any error

echo "===== Lambda GPU Instance Setup Script ====="
echo "Starting setup at $(date)"
echo ""

# Update package lists
echo "Updating package lists..."
sudo apt-get update

# Install system dependencies
echo "Installing system dependencies..."
sudo apt-get install -y \
    build-essential \
    python3-dev \
    python3-pip \
    git \
    wget \
    curl \
    software-properties-common \
    ninja-build \
    libopenblas-dev \
    htop \
    tmux

# Upgrade pip
echo "Upgrading pip..."
python3 -m pip install --upgrade pip

# Create a virtual environment
echo "Creating Python virtual environment..."
python3 -m pip install --user virtualenv
python3 -m virtualenv ~/llm_env

# Activate the virtual environment
echo "Activating virtual environment..."
source ~/llm_env/bin/activate

# Install PyTorch with CUDA support
echo "Installing PyTorch with CUDA support..."
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

# Install LLM-related packages
echo "Installing LLM packages..."
pip install \
    transformers \
    accelerate \
    bitsandbytes \
    peft \
    einops \
    safetensors \
    sentencepiece \
    scipy \
    protobuf \
    tensorboard \
    pyyaml \
    tqdm \
    matplotlib \
    datasets \
    huggingface_hub

# Install SERP processing dependencies
echo "Installing SERP processing dependencies..."
pip install \
    requests \
    pandas \
    beautifulsoup4 \
    lxml \
    numpy \
    python-dotenv \
    prometheus-client

# Create project directories
echo "Creating project directories..."
mkdir -p ~/SauceScrapero3

# Clone or pull project code
# (Uncomment and modify if you have a repository to clone)
# echo "Cloning project repository..."
# if [ -d "~/SauceScrapero3/.git" ]; then
#     cd ~/SauceScrapero3
#     git pull
# else
#     git clone https://github.com/yourusername/your-repo.git ~/SauceScrapero3
# fi

# Download Qwen-3 model (modify as needed)
echo "Note: No models are being downloaded by default."
echo "To download Qwen-3-7B, uncomment the code in this script or run:"
echo "  python -c \"from huggingface_hub import snapshot_download; snapshot_download('Qwen/Qwen3-7B')\""

# ==== Uncomment to download models automatically ====
# echo "Downloading Qwen-3-7B model (this may take a while)..."
# python -c "from huggingface_hub import snapshot_download; snapshot_download('Qwen/Qwen3-7B')"

# Create a script to activate the environment easily
echo "Creating activation script..."
cat > ~/activate_llm_env.sh << 'EOF'
#!/bin/bash
source ~/llm_env/bin/activate
echo "LLM environment activated"
EOF

chmod +x ~/activate_llm_env.sh

# Final setup message
echo ""
echo "===== Setup Complete ====="
echo "To activate the environment, run: source ~/activate_llm_env.sh"
echo "GPU information:"
nvidia-smi
echo ""
echo "Environment setup completed at $(date)" 