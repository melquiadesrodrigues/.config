#!/usr/bin/env zsh

selected_session_name="default"
tmux_running=$(pgrep tmux)

# Case 1: Not inside tmux and tmux isn't running — start a new session
if [[ -z $TMUX ]] && [[ -z $tmux_running ]]; then
    if ! tmux has-session -t=$selected_session_name 2>/dev/null; then
        exec tmux new-session -s $selected_session_name
    else
        exec tmux attach -t $selected_session_name
    fi
fi

# Case 2: Already inside tmux — don't switch if already in the session
if [[ -n $TMUX ]]; then
    current_session=$(tmux display-message -p '#S')
    if [[ "$current_session" == "$selected_session_name" ]]; then
        exit 0
    fi
fi

# Ensure the session exists
if ! tmux has-session -t=$selected_session_name 2>/dev/null; then
    tmux new-session -ds $selected_session_name -n "editor"
fi

# Switch only if inside tmux and not already in target session
if [[ -n $TMUX ]]; then
    tmux switch-client -t $selected_session_name
else
    tmux attach -t $selected_session_name
fi

