# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import time

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
    ds=time.strftime('%Y%m%d')
    y_ds=time.strftime('%Y%m%d')
    print(ds,y_ds)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/


from datetime import date, timedelta
today = date.today()
yesterday = (today - timedelta(days = 1)).strftime('%Y%m%d')
print(today.strftime('%Y%m%d'))
print(yesterday)