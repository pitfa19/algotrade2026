# AlgoTrade 2026 — Network & SSH Guide¶

A short companion to the Participant Guide covering how to get on the venue network and use your team VM.

* * *

## 1) Network Access¶

Two ways to get on the venue network:

  * **Wired (recommended).** Each team's table has a switch with Ethernet ports — plug in and you are on. Wired is more stable and we strongly recommend it.
  * **WiFi.** Each team is given a per-team WiFi login at the start of the event.



Once you are on the network, the following names resolve:

  * `vm.algotrade.hr` — your team's VM (SSH)
  * `dashboard.algotrade.hr` — trading UI
  * `<exchange>.algotrade.hr` — the 10 exchanges (full list in the Participant Guide §7)



* * *

## 2) Connecting to Your VM¶

**Credentials**

  * Host: `vm.algotrade.hr`
  * User: `root`
  * Password: `algotrade`



### Linux / macOS¶

From a terminal:
    
    
    ssh root@vm.algotrade.hr
    

### Windows¶

Modern Windows ships with OpenSSH. Open PowerShell and run:
    
    
    ssh root@vm.algotrade.hr
    

If `ssh` is not available, install **OpenSSH Client** from _Settings → Apps → Optional Features_ , or use PuTTY (Host: `vm.algotrade.hr`, Port: 22).

* * *

## 3) Running Processes in the Background¶

A plain SSH session ends when your laptop sleeps or the network blips. Run long-lived processes inside `tmux` so they survive disconnects.
    
    
    # Start a named session and run your bot in it
    tmux new -s bot
    python my_bot.py
    
    # Detach (leaves the bot running)
    Ctrl-b  d
    
    # Later — log back in and re-attach
    ssh root@vm.algotrade.hr
    tmux attach -t bot
    

Useful commands inside tmux (all start with `Ctrl-b`):

Keys | Action  
---|---  
`Ctrl-b d` | Detach (session keeps running)  
`Ctrl-b "` | Split horizontally  
`Ctrl-b %` | Split vertically  
`Ctrl-b o` | Switch pane  
`Ctrl-b [` | Scroll mode (`q` to exit)  
  
`tmux ls` lists existing sessions. `screen` is a similar alternative if you prefer it.

* * *

## 4) Transferring Files¶

### Linux / macOS¶
    
    
    # Push a file
    scp my_bot.py root@vm.algotrade.hr:~/
    
    # Push a directory
    scp -r ./bot root@vm.algotrade.hr:~/
    
    # Pull a file back
    scp root@vm.algotrade.hr:~/market_data/NYSE_trades.csv ./
    
    # Sync a directory both ways (faster for repeated copies)
    rsync -avz ./bot/ root@vm.algotrade.hr:~/bot/
    rsync -avz root@vm.algotrade.hr:~/market_data/ ./market_data/
    

`sftp root@vm.algotrade.hr` opens an interactive session if you want to browse and pick files.

### Windows¶

`scp` works the same in PowerShell:
    
    
    scp my_bot.py root@vm.algotrade.hr:~/
    scp root@vm.algotrade.hr:~/market_data/NYSE_trades.csv .
    

For drag-and-drop, WinSCP is the easiest option — connect with protocol _SFTP_ , host `vm.algotrade.hr`, user `root`.

### Graphical clients (any OS)¶

  * **WinSCP** (Windows)
  * **Cyberduck** or **Transmit** (macOS) — connect via _SFTP_
  * **FileZilla** (cross-platform) — pick _SFTP_ , not FTP
  * **VS Code** with the _Remote - SSH_ extension — edit files on the VM directly



* * *
