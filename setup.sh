#!/usr/bin/env bash

set -e

is_macos() {
    [[ "$(uname)" == "Darwin" ]]
}

is_linux() {
    [[ "$(uname)" == "Linux" ]]
}

install_package() {
    local package=$1
    if is_macos; then
        if ! command -v brew &>/dev/null; then
            echo "Installing Homebrew..."
            /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        fi
        if command -v $package &>/dev/null || brew list "$package" &>/dev/null; then
            echo "$package package is already installed"
        else
            echo "Installing $package..."
            brew install "$package"
        fi
    elif is_linux; then
        sudo apt-get update -y
        if command -v $package &>/dev/null || dpkg -s "$package" &>/dev/null; then
            echo "$package package is already installed"
        else
            echo "Installing $package..."
            sudo apt-get install -y "$package"
        fi
    else
        echo "Unsupported OS"
        exit 1
    fi
}

backup_and_copy() {
    local src=$1
    local dest="$HOME/$(basename "$src")"
    if [[ -e "$dest" && ! -e "$dest.bak" ]]; then
        echo "Backing up $dest to $dest.bak"
        mv "$dest" "$dest.bak"
    fi
    echo "Copying $src to $dest"
    cp "$src" "$dest"
}

apply_custom_profile() {
    local dotfiles_dir=$1
    sed -i "" "/.custom_profile/d" ~/.zprofile > /dev/null 2>&1
    echo "source $(pwd)/$dotfiles_dir/.custom_profile" | tee -a ~/.zprofile > /dev/null 2>&1
}

install_oh_my_zsh() {
    if [[ ! -d "$HOME/.oh-my-zsh" ]]; then
        echo "Installing oh-my-zsh..."
        RUNZSH=no KEEP_ZSHRC=yes sh -c \
            "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"
    else
        echo "oh-my-zsh is already installed."
    fi
}

install_oh_my_zsh_plugin() {
    local plugin=$1
    local repo=$2
    if [[ ! -d "${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/plugins/$plugin" ]]; then
        echo "Installing $plugin plugin..."
        git clone "https://github.com/$repo" "${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/plugins/$plugin"
    else
        echo "$plugin plugin is already installed."
    fi
}

install_p10k() {
    local theme_dir="${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k"
    if [[ ! -d "$theme_dir" ]]; then
        echo "Installing Powerlevel10k..."
        git clone --depth=1 https://github.com/romkatv/powerlevel10k.git "$theme_dir"
    else
        echo "Powerlevel10k is already installed."
    fi
}

install_neovim() {
    if ! command -v nvim &>/dev/null; then
        echo "Installing neovim..."
        if is_macos; then
            brew install neovim
        else
            curl -LO https://github.com/neovim/neovim/releases/latest/download/nvim-linux-x86_64.tar.gz
            sudo rm -rf /opt/nvim
            sudo tar -C /opt -xzf nvim-linux-x86_64.tar.gz
            echo 'export PATH="$PATH:/opt/nvim-linux-x86_64/bin"' >> $HOME/.zshrc
        fi
    else
        echo "Neovim is already installed."
    fi
}

install_custom_scripts() {
    local scripts_dir="$HOME/scripts"
    if [[ ! -d "$scripts_dir" ]]; then
        echo "Installing custom scripts"
        git clone git@github.com:melquiadesrodrigues/scripts.git "$scripts_dir" \
            || git clone https://github.com/melquiadesrodrigues/scripts.git
        chmod +x "$scripts_dir/register-commands.sh"
        "$scripts_dir/register-commands.sh"
    else
        echo "Custom scripts are already installed."
    fi
}

main() {
    # Assume dotfiles are in a folder named 'dotfiles' in current directory
    DOTFILES_DIR="dotfiles"

    cd $( cd "$(dirname "$0")" ; pwd -P )

    apply_custom_profile "$DOTFILES_DIR"
    backup_and_copy "$DOTFILES_DIR/.p10k.zsh"
    backup_and_copy "$DOTFILES_DIR/.zshrc"

    install_package git
    install_package zsh
    install_package ripgrep
    install_package tmux
    install_oh_my_zsh
    install_oh_my_zsh_plugin "zsh-autosuggestions" "zsh-users/zsh-autosuggestions"
    install_oh_my_zsh_plugin "zsh-syntax-highlighting" "zsh-users/zsh-syntax-highlighting"
    install_p10k
    install_neovim
    install_custom_scripts
}

main "$@"
