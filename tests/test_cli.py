import pytest
import subprocess

def test_fake_hisepy_cli():
    # Run the command
    result = subprocess.run(['python', '-m', 'fake_hisepy', '--about',],
                            capture_output=True, text=True)

    # Check the output
    assert 'This is a program that provides information based on command line arguments.' in result.stdout