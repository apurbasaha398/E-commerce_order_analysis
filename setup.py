from setuptools import find_packages, setup
from typing import List

HYPEN_E_DOT = "-e ."

def get_requirements(file_path:str)->List[str]:
    '''
    Read the requirements.txt file and return the list of requirements
    '''
    requirements = []
    with open(file_path, "r") as file_obj:
        requirements = file_obj.read().splitlines()
        
        if HYPEN_E_DOT in requirements:
            requirements.remove(HYPEN_E_DOT)
    return requirements
    
setup(
    name="olist",
    version='0.0.1',
    author="Apurba",
    author_email="apurba.saha.ipe@gmail.com",
    packages=find_packages(),
    install_requires=get_requirements('requirements.txt')
)