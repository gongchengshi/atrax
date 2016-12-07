from jinja2 import Environment, FileSystemLoader
from os import path
from atrax.management.aws_env.constants import PACKAGES_TAG_NAME


template_dir = path.join(path.dirname(path.realpath(__file__)), 'user_data')


def generate_upstart_script(crawl_job_name, module_name):
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('atrax.conf.jinja2')
    with open(path.join(template_dir, 'update_module.sh'), 'r') as update_module_file:
        update_module_script = update_module_file.read()
    script = template.render(crawl_job=crawl_job_name,
                             module_name=module_name, update_module_script=update_module_script)
    return script


def generate_spot_instance_init_script(crawl_job_name, modules):
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('spot_instance_init.sh.jinja2')
    script = template.render(crawl_job=crawl_job_name, modules=modules, packages_tag_name=PACKAGES_TAG_NAME)
    return script


def generate_cloud_config():
    with open(path.join(template_dir, 'cloud_config.yaml'), 'r') as cloud_config_file:
        return cloud_config_file.read()


def generate_stopgap_debian_setup():
    """
    Additional setup that hasn't yet been added to the AMI
    """
    with open(path.join(template_dir, 'stopgap_debian_setup.sh'), 'r') as stopgap_debian_setup_file:
        return stopgap_debian_setup_file.read()


import sys

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def create_multipart(parts):
    combined_message = MIMEMultipart()
    for filename, format_type, contents in parts:
        if not contents.strip():
            continue
        sub_message = MIMEText(contents, format_type, sys.getdefaultencoding())
        sub_message.add_header('Content-Disposition', 'attachment; filename="%s"' % filename)
        combined_message.attach(sub_message)
    return combined_message.as_string()
