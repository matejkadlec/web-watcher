from settings import init_settings
from tkinter import *

win = Tk()

win.title("ScrapeMonitor settings")
win.geometry('420x280')

url_lbl = Label(win, text="URL", width=15, anchor=E)
url_lbl.grid(column=0, row=0, pady=10, padx=10)
url_ent = Entry(win, width=40)
url_ent.insert(0, 'https://example.com/')
url_ent.grid(column=1, row=0)

response_lbl = Label(win, text="Response", width=15, anchor=E)
response_lbl.grid(column=0, row=1, padx=10)
response_ent = Entry(win, width=4)
response_ent.insert(0, '8')
response_ent.grid(column=1, row=1, sticky=W)

title_lbl = Label(win, text="Title", width=15, anchor=E)
title_lbl.grid(column=0, row=2, pady=10, padx=10)
title_ent = Entry(win, width=4)
title_ent.insert(0, '8')
title_ent.grid(column=1, row=2, sticky=W)

description_lbl = Label(win, text="Description", width=15, anchor=E)
description_lbl.grid(column=0, row=3, padx=10)
description_ent = Entry(win, width=4)
description_ent.insert(0, '8')
description_ent.grid(column=1, row=3, sticky=W)

robots_lbl = Label(win, text="Robots", width=15, anchor=E)
robots_lbl.grid(column=0, row=4, pady=10, padx=10)
robots_ent = Entry(win, width=4)
robots_ent.insert(0, '8')
robots_ent.grid(column=1, row=4, sticky=W)

image_lbl = Label(win, text="Image", width=15, anchor=E)
image_lbl.grid(column=0, row=5, padx=10)
image_ent = Entry(win, width=4)
image_ent.insert(0, '8')
image_ent.grid(column=1, row=5, sticky=W)

content_lbl = Label(win, text="Content", width=15, anchor=E)
content_lbl.grid(column=0, row=6, pady=10, padx=10)
content_ent = Entry(win, width=4)
content_ent.insert(0, '8')
content_ent.grid(column=1, row=6, sticky=W)


def submit_settings():
    init_settings(url_ent.get(), [response_ent.get(), title_ent.get(), description_ent.get(), robots_ent.get(),
                                  image_ent.get(), content_ent.get()])
    win.destroy()


submit = Button(win, text="Submit", command=submit_settings)
submit.grid(column=1, row=7, sticky=E, padx=5, pady=2)

win.mainloop()
