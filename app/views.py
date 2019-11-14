from flask import Blueprint, render_template, abort
from jinja2 import TemplateNotFound

list_page = Blueprint('list_page', 'list_page', template_folder='templates')


@list_page.route('/list')
def get_sample():
    try:
        return render_template('index.html')
    except TemplateNotFound:
        abort(404)
