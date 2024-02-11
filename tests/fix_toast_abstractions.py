from re import M
import sys
from tempfile import TemporaryDirectory

sys.path.insert(0, '../')

import os
import pytest
import requests
from unittest import mock
import tempfile

from fake_hisepy.data_apps.abstraction import AbstractionAppImg

class TestAbstractionAppImg:

    @pytest.fixture
    def init_test(self):
        # create temporary directory
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmpdirname = self.tmpdir.name

        # create temporary files and instantiate abstractionAppImg
        self.app_path = os.path.normpath("{}/app.py".format(self.tmpdirname))
        self.img_path = os.path.normpath("{}/img.png".format(self.tmpdirname))
        os.system("touch {}/app.py".format(self.tmpdirname))
        os.system("touch {}/img.png".format(self.tmpdirname))
        self.abstraction_img = AbstractionAppImg(
            app_filepath=self.app_path,
            hero_image=self.img_path,
            title='test abstraction',
            description='a description worth reading',
            data_contract_id='fakeGUID123',
            project_guid='projgu12d',
            work_dir=self.tmpdirname)

        # create tarball
        self.abstraction_img.copy_files_to_tmp(
            self.abstraction_img.abstraction_config_filenames)
        self.abstraction_img.create_tarball()

    def cleanup(self, init_test):
        self.tmpdir.cleanup()

    # arrange mock object for post request to toolchain
    @pytest.fixture
    def mock_post(self):
        mock = Mock()
        mock.patch("requests.post", return_value=mock)
        return mock

    # arrange mock object for params
    @pytest.fixture
    def post_params(self):
        return {
            "title": "mock title",
            "description": "descibing what an abstraction is",
            "inputResultFiles": ['d2700632-4ce8-44df-95ba-9290be3c86b6'],
            "projectGuid": 'project123',
            "notebook": "mock_notebook.ipynb",
            "appDetails": "random text",
            "homedir": "/home/jupyter",
            "instanceId": "user-ide-id"
        }

    @pytest.fixture
    def create_file_arg(self):
        return self.abstraction_img.create_file_arg()

    @pytest.fixture
    def create_url(self, post_params, init_test):
        return self.abstraction_img.create_url(post_params)

    def test_post_request(self, create_url, create_file_arg, mocker):

        mock_post = mocker.patch('requests.post')
        mock_response = mock_post.return_value
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "success"}

        # send it
        resp = self.abstraction_img.send_post(create_url, create_file_arg)

        # assertion checks
        assert resp.status_code == 200
        assert resp.json() == {"status": "success"}
        return

    def test_tarball_creation(self, init_test):
        # init_test() method should have created the tarball, so we just check for it
        assert os.path.isfile('{}/{}'.format(
            self.tmpdirname, self.abstraction_img.abstraction_image_name))
        return

    def test_post_static_image(self, mocker, init_test):
        mock_post = mocker.patch('requests.post')
        mock_response = mock_post.return_value
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "success"}

        # send it
        resp = self.abstraction_img.send_static_image_post(
            self.abstraction_img.create_static_image_url(),
            self.abstraction_img.create_image_dict())

        # assertion checks
        assert resp.status_code == 200
        assert resp.json() == {"status": "success"}
        return

    def test_config_files_exist(self, init_test):
        for f in self.abstraction_img.abstraction_config_filenames:
            assert os.path.isfile('{}/{}'.format(self.abstraction_img.work_dir,
                                                 f))

    def test_cleanup(self, init_test):
        self.cleanup(init_test)
        assert os.path.isdir(self.abstraction_img.work_dir) == False
