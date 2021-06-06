
from flask import Flask, render_template
from flask_socketio import SocketIO, emit
import os, markdown

pylib_path = os.path.dirname(os.path.realpath(__file__))

workbooks = {}

app = Flask(__name__, template_folder=pylib_path)
socketio = SocketIO(app)

@app.route('/')
def index():
    crumbs = [ ('Workbooks','/') ]
    return render_template('flask_app.html', title='Workbook', crumbs=crumbs, workbooks = workbooks)

@app.route('/workbook/<workbookName>')
def get_workbook(workbookName):
    contents_md = open(workbooks[workbookName], 'r').read()
    contents_html = markdown.markdown(contents_md, extensions=['tables','extra','codehilite'])

    crumbs = [ ('Workbooks','/'),(workbookName, '/workbook/'+workbookName) ]
    return render_template('flask_app.html', title='Workbook: '+workbookName, crumbs=crumbs, contents = contents_html)


@socketio.on('my event')
def test_message(message):
    emit('my response', {'data': message['data']})

@socketio.on('my broadcast event')
def test_message(message):
    emit('my response', {'data': message['data']}, broadcast=True)

@socketio.on('connect')
def test_connect():
    emit('my response', {'data': 'Connected'})

@socketio.on('disconnect')
def test_disconnect():
    print('Client disconnected')

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class MyHandler(FileSystemEventHandler):
    def on_modified(self, event):
        print(f'event type: {event.event_type}  path : {event.src_path}')
        socketio.emit('my_response', {'data':'Hello, ' + event.src_path})

def LoadWorkbook(workbookPath):

    base = os.path.basename(workbookPath)
    baseWoExt = os.path.splitext(base)[0]

    workbooks[baseWoExt] = workbookPath


def Run():
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path='.', recursive=False)
    observer.start()

    socketio.run(app)

    observer.stop()

if __name__ == "__main__":
    Run()
