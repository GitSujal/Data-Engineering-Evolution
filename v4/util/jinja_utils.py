import jinja2
import json

config_file = "config.json"

def render_config(config_file, **kwargs):
    """Render a config file using jinja2"""
    with open(config_file, 'r') as f:
        template = jinja2.Template(f.read())
    return template.render(**kwargs)

def read_rendered_config(config_file, profile_name:str ="local", **kwargs):
    """Read a rendered config file"""
    config = render_config(config_file, **kwargs)
    config_dict = json.loads(config)
    config = None
    for profile in config_dict['profiles']:
        if profile['name'] == profile_name:
            config = profile
    return config

if __name__ == "__main__":
    kwargs = {"job": "data-analyst", "location": "australia", 
              "job_first_key": "data"}
    config = read_rendered_config(config_file, profile_name="remote-dev", **kwargs)
    print(config)
    print(type(config))