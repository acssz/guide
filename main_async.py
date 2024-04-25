import asyncio
import logging
import os.path
from asyncio import Task
from typing import List, TypedDict
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


class LarkOpenApiError(Exception):
    def __init__(self, code: int, msg: str):
        super().__init__(self, code, msg)
        self.__code = code
        self.__msg = msg

    @property
    def code(self):
        return self.__code

    @property
    def msg(self):
        return self.__msg

    def __str__(self) -> str:
        return f'LarkOpenApiError: {self.code} {self.msg}'


class DocTreeNode(object):
    def __init__(self, value: Node | None = None, parent: 'DocTreeNode' = None):
        self.__value = value
        self.__parent: DocTreeNode | None = parent
        self.__children: List[DocTreeNode] = []

    def add_child(self, child: 'DocTreeNode'):
        self.__children.append(child)

    @property
    def value(self):
        return self.__value

    @property
    def parent(self):
        return self.__parent

    @property
    def children(self):
        return self.__children

    @property
    def nodes(self):
        class DFS:
            def __init__(self):
                self.nodes = []

            def walk(self, node: DocTreeNode) -> List[DocTreeNode]:
                self.nodes.append(node)
                for child in node.children:
                    self.walk(child)
                return self.nodes

        return DFS().walk(self)


class DocTreeWalker(object):
    class UninitializedException(Exception):
        pass

    def __init__(self, client: lark.Client):
        self.__doc_tree_root = DocTreeNode(Node({
            "title": "root"
        }))
        self.__client = client
        self.__space_id = None

    async def walk(self, space_id: str, parent_node: DocTreeNode | None = None) -> None:
        from lark_oapi.api.wiki.v2 import ListSpaceNodeRequest, ListSpaceNodeRequestBuilder, ListSpaceNodeResponse

        if not space_id == self.__space_id:
            self.__doc_tree_root = DocTreeNode()
            self.__space_id = space_id

        parent_node_token: str | None = None
        if parent_node:
            parent_node_token = parent_node.value.node_token
        else:
            parent_node = self.__doc_tree_root

        page_token: str | None = None
        while True:
            builder: ListSpaceNodeRequestBuilder = ListSpaceNodeRequestBuilder().space_id(space_id)
            if page_token:
                builder = builder.page_token(page_token)
            if parent_node_token:
                builder = builder.parent_node_token(parent_node_token)
            req: ListSpaceNodeRequest = builder.build()
            resp: ListSpaceNodeResponse = self.__client.wiki.v2.space_node.list(req)
            if not resp.success():
                logging.error(f'failed to dispatch list space nodes request: {resp.code} {resp.msg}')
                raise LarkOpenApiError(resp.code, resp.msg)

            # dive into current node
            subtree_tasks = []
            if resp.data.items:
                for item in resp.data.items:
                    logging.info(f'found document: title={item.title}, type={item.obj_type}')
                    node = DocTreeNode(item)
                    parent_node.add_child(node)
                    subtree_task = asyncio.create_task(self.walk(space_id, node))
                    subtree_tasks.append(subtree_task)
            for task in subtree_tasks:
                await task

            if resp.data.has_more:
                page_token = resp.data.page_token
            else:
                break

    @property
    def tree_root(self) -> DocTreeNode:
        if not self.__space_id:
            raise self.UninitializedException()
        return self.__doc_tree_root


async def create_export_task(client: lark.Client, node: Node) -> str:
    from lark_oapi.api.drive.v1 import ExportTaskBuilder, CreateExportTaskRequestBuilder

    task = ExportTaskBuilder().file_extension('pdf').token(
        node.obj_token).type(node.obj_type).build()
    req = CreateExportTaskRequestBuilder().request_body(task).build()
    resp = client.drive.v1.export_task.create(req)
    if not resp.success():
        logging.error(
            f'failed to dispatch create export task request: {resp.code} {resp.msg}')
        raise LarkOpenApiError(resp.code, resp.msg)
    return resp.data.ticket


async def wait_task(client: lark.Client, node: Node, ticket: str) -> ExportTask:
    from lark_oapi.api.drive.v1 import GetExportTaskRequestBuilder

    running = True
    while running:
        req = GetExportTaskRequestBuilder().token(node.node_token).ticket(ticket).build()
        resp = client.drive.v1.export_task.get(req)
        if not resp.success():
            logging.error(
                f'failed to query task execution status: {resp.code} {resp.msg}')
            raise LarkOpenApiError(resp.code, resp.msg)
        status, msg = resp.data.result.job_status, resp.data.result.job_error_msg
        if status == 0:
            return resp.data.result
        elif status != 1 and status != 2:
            logging.error(f'job failed: {node.title}: {status} {msg}')
            raise LarkOpenApiError(status, msg)


async def download_exported_pdf(client: lark.Client, task: ExportTask, path: str) -> str:
    from lark_oapi.api.drive.v1 import DownloadExportTaskRequestBuilder

    req = DownloadExportTaskRequestBuilder().file_token(task.file_token).build()
    resp = client.drive.v1.export_task.download(req)
    if not resp.success():
        logging.error(f'failed to download exported file: {resp.code} {resp.msg}')
        raise LarkOpenApiError(resp.code, resp.msg)
    with open(path, 'wb') as f:
        f.write(resp.raw.content)
    return path


async def main():
    logging.basicConfig(level=logging.INFO)
    client = lark.Client.builder().app_id(
        LARK_APP_ID).app_secret(LARK_APP_SECRET).build()
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    # retrieve nodes
    logging.info(f'space_id={SPACE_ID}')
    walker = DocTreeWalker(client)
    await walker.walk(SPACE_ID)
    nodes = walker.tree_root.nodes[1:]  # remove dummy root node

    # dispatch export tasks
    class ExportJob(TypedDict):
        ticket: Task[str]
        node: DocTreeNode
        result: Task[ExportTask] | None

    jobs: List[ExportJob] = []
    for node in nodes:
        jobs.append({
            'ticket': asyncio.create_task(create_export_task(client, node.value)),
            'node': node,
            'result': None,
        })
        logging.info(f'export task created: title={node.value.title}')

    # wait until all executions of all tasks are finished
    for job in jobs:
        job['result'] = asyncio.create_task(wait_task(client, job['node'].value, await job['ticket']))
        logging.info(f'task finished: {job["node"].value.title}')

    # download exported files
    import tempfile
    tempdir = tempfile.mkdtemp()

    download_tasks: List[Task[str]] = []
    for index, job in enumerate(jobs):
        result: ExportTask = await job['result']
        path = os.path.join(tempdir, f'{index}.pdf')
        download_tasks.append(asyncio.create_task(download_exported_pdf(client, result, path)))
        logging.info(f'exported pdf downloaded: {job["node"].value.title}')
    await asyncio.gather(*download_tasks)

    # concatenate pdfs
    import pypdf

    with pypdf.PdfWriter() as merger:
        if COVER_PDF_PATH:
            merger.append(COVER_PDF_PATH)
        for job in download_tasks:
            merger.append(await job)
        merger.write(os.path.join(OUTPUT_DIR, 'guide.pdf'))

    # cleanup temporary directory
    shutil.rmtree(tempdir)


if __name__ == '__main__':
    asyncio.run(main())
