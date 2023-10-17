import tableauserverclient as TSC
import logging


class TableauServerUtils:
    def __init__(self, server_address: str, token_name: str, token_value: str) -> None:
        """TableauServerUtils constructor

        Args:
            server_address (str): tableau server address
            token_name (str): token name
            token_value (str): token value
        """
        self.server = TSC.Server(server_address, use_server_version=True)
        self.tableau_auth = TSC.PersonalAccessTokenAuth(token_name, token_value)

    def get_project_id(self, project_name: str) -> str:
        """Get project id by name

        Args:
            project_name (str): project name

        Raises:
            ValueError: raises error whether the functions find more than 1
             project with the same project_name

        Returns:
            str: project id
        """
        logging.info(f"Signing into the server {self.server.baseurl}")
        with self.server.auth.sign_in(self.tableau_auth):
            req_options = TSC.RequestOptions()
            req_options.filter.add(
                TSC.Filter(
                    TSC.RequestOptions.Field.Name,
                    TSC.RequestOptions.Operator.Equals,
                    project_name,
                )
            )
            projects = list(TSC.Pager(self.server.projects, req_options))
            if len(projects) > 1:
                raise ValueError("The project name is not unique.")
        return projects[0].id

    def publish_hyper(
        self, project_id: str, hyper_path: str, mode: str = "overwrite"
    ) -> None:
        """Publish hyper file into the tableau server

        Args:
            project_id (str): project id
            hyper_path (str): hyper file path. Eg: path/hyper.file
            mode (str): publish mode. Accept overwrite or append mode.
            Defaults to overwrite.
        """
        OVERWRITE = "overwrite"
        APPEND = "append"
        logging.info(f"Signing into the server {self.server.baseurl}")
        with self.server.auth.sign_in(self.tableau_auth):
            if mode == OVERWRITE:
                publish_mode = TSC.Server.PublishMode.Overwrite
            elif mode == APPEND:
                publish_mode = TSC.Server.PublishMode.Append
            else:
                raise ValueError(f"Mode must be overwrite or append. Received {mode}")
            datasource = TSC.DatasourceItem(project_id=project_id)
            logging.info("Publishing Hyper file into the server!")
            ds = self.server.datasources.publish(datasource, hyper_path, publish_mode)
            logging.info(f"Datasource published on ID: {ds.id}")
        logging.info("Job Finished.")
