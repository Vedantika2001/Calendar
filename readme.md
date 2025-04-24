Step 1: Make the Shell Script Executable

'''bash
chmod +x run_script.sh
'''

Step 2: Schedule the Cron Job

Open your crontab file for editing :

'''bash
crontab -e
'''

Add a line to schedule your shell script. For example, if you want to run it every day at 10 AM, you would add:

'''bash
0 10 * * * /path/to/your/run_script.sh
'''
