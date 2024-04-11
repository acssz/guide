import logging
import os.path
from typing import List
import shutil

import lark_oapi as lark
from lark_oapi.api.drive.v1 import ExportTask
from lark_oapi.api.wiki.v2 import Node


# mandatory environment variables
LARK_APP_ID = os.environ.get('LARK_APP_ID')
LARK_APP_SECRET = os.environ.get('LARK_APP_SECRET')

# optional environment variables
SPACE_ID = '7355566671643279392'
if 'SPACE_ID' in os.environ:
    SPACE_ID = os.environ.get('SPACE_ID')
COVER_PDF_PATH = os.environ.get('COVER_PDF_PATH')
OUTPUT_DIR = 'out'
if 'OUTPUT_DIR' in os.environ:
    OUTPUT_DIR = os.environ.get('OUTPUT_DIR')


class ApiException(Exception):
    def __init__(self, code: int, msg: str):
        self.__code = code
        self.__msg = msg
        super(f'{self.__code} {self.__msg}')

    @property
    def code(self):
        return self.__code

    @property
    def msg(self):
        return self.__msg


class NodeTreeWalker(object):
    class UninitializedException(Exception):
        pass

    def __init__(self, client: lark.Client):
        self.__nodes = []
        self.__client = client
        self.__space_id = None

    def walk(self, space_id: str, parent_node_token: str | None = None) -> List[Node]:
        from lark_oapi.api.wiki.v2 import ListSpaceNodeRequest, ListSpaceNodeRequestBuilder, ListSpaceNodeResponse

        if not space_id == self.__space_id:
            self.__nodes = []
            self.__space_id = space_id

        page_token: str | None = None
        while True:
            builder = ListSpaceNodeRequestBuilder().space_id(space_id)
            if page_token:
                builder = builder.page_token(page_token)
            if parent_node_token:
                builder = builder.parent_node_token(parent_node_token)
            req: ListSpaceNodeRequest = builder.build()
            resp: ListSpaceNodeResponse = self.__client.wiki.v2.space_node.list(
                req)
            if not resp.success():
                logging.error(
                    f'failed to dispatch list space nodes request: {resp.code} {resp.msg}')
                raise ApiException(resp.code, resp.msg)

            # dive into current node
            if resp.data.items:
                for item in resp.data.items:
                    logging.info(
                        f'found document: {item.title}, {item.obj_type}')
                    self.__nodes.append(item)
                    self.walk(space_id, item.node_token)

            if resp.data.has_more:
                page_token = resp.data.page_token
            else:
                break

        return self.nodes

    @property
    def nodes(self) -> List[Node]:
        if not self.__space_id:
            raise self.UninitializedException()
        return self.__nodes


def create_export_task(client: lark.Client, node: Node) -> str:
    from lark_oapi.api.drive.v1 import ExportTaskBuilder, CreateExportTaskRequestBuilder

    task = ExportTaskBuilder().file_extension('pdf').token(
        node.obj_token).type(node.obj_type).build()
    req = CreateExportTaskRequestBuilder().request_body(task).build()
    resp = client.drive.v1.export_task.create(req)
    if not resp.success():
        logging.error(
            f'failed to dispatch create export task request: {resp.code} {resp.msg}')
        raise ApiException(resp.code, resp.msg)
    return resp.data.ticket


def wait_task(client: lark.Client, node: Node, ticket: str) -> ExportTask:
    from lark_oapi.api.drive.v1 import GetExportTaskRequestBuilder

    running = True
    while running:
        req = GetExportTaskRequestBuilder().token(
            node.node_token).ticket(ticket).build()
        resp = client.drive.v1.export_task.get(req)
        if not resp.success():
            logging.error(
                f'failed to query task execution status: {resp.code} {resp.msg}')
            raise ApiException(resp.code, resp.msg)
        status, msg = resp.data.result.job_status, resp.data.result.job_error_msg
        if status == 0:
            return resp.data.result
        elif status != 1 and status != 2:
            logging.error(f'job failed: {status} {msg}')
            raise ApiException(status, msg)


def download_exported_pdf(client: lark.Client, task: ExportTask, path: str):
    from lark_oapi.api.drive.v1 import DownloadExportTaskRequestBuilder

    req = DownloadExportTaskRequestBuilder().file_token(task.file_token).build()
    resp = client.drive.v1.export_task.download(req)
    if not resp.success():
        logging.error(
            f'failed to download exported file: {resp.code} {resp.msg}')
        raise ApiException(resp.code, resp.msg)
    with open(path, 'wb') as f:
        f.write(resp.raw.content)


def main():
    logging.basicConfig(level=logging.INFO)
    client = lark.Client.builder().app_id(
        LARK_APP_ID).app_secret(LARK_APP_SECRET).build()
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    # retrieve nodes
    logging.info(f'space_id={SPACE_ID}')
    walker = NodeTreeWalker(client)
    nodes = walker.walk(SPACE_ID)

    # dispatch export tasks
    tasks = []
    for node in nodes:
        ticket = create_export_task(client, node)
        tasks.append({
            'ticket': ticket,
            'node': node,
        })
        logging.info(f'export task created: {node.title}')

    # wait until all executions of all tasks are finished
    for task in tasks:
        result = wait_task(client, **task)
        task['result'] = result
        logging.info(f'task finished: {task["node"].title}')

    # download exported files
    import tempfile
    tempdir = tempfile.mkdtemp()

    for index, task in enumerate(tasks):
        result: ExportTask = task['result']
        path = os.path.join(tempdir, f'{index}.pdf')
        download_exported_pdf(client, result, path)
        logging.info(f'exported pdf downloaded: {task["node"].title}')

    # concatenate pdfs
    import pypdf

    with pypdf.PdfWriter() as merger:
        if COVER_PDF_PATH:
            merger.append(COVER_PDF_PATH)
        for index, task in enumerate(tasks):
            merger.append(os.path.join(tempdir, f'{index}.pdf'))
        merger.write(os.path.join(OUTPUT_DIR, 'guide.pdf'))

    # cleanup temporary directory
    shutil.rmtree(tempdir)


if __name__ == '__main__':
    main()
