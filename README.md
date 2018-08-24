# ControlMToFlowChart

Generate Flowchart from Control M Schedule
------------------------------------------
This code generates job flow diagram by reading raw text present in Control M Flow. This connects to a remote using FTP connection with credentials. Update below lines per your configuration
    
    
    ftp = FTP('mainframe.ip.address')                                
    ftp.login(u, p)  
    
