import asyncio
import logging
import os.path
from asyncio import Task
from typing import List, TypedDict
import shutil
import time
import random
import pymupdf
from pathlib import Path

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


def exponential_backoff(max_retries: int = 3, base_delay: int = 1):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            from requests import exceptions

            retries = 0
            def retry():
                nonlocal retries
                retries += 1
                print(f"Attempt {retries} failed: {e}")
                delay = (base_delay * 2 ** retries + random.uniform(0, 1))
                print(f"Retrying in {delay:.2f} seconds...")
                time.sleep(delay)

            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except LarkOpenApiError as e:
                    # 所有错误码：https://open.larksuite.com/document/server-docs/getting-started/server-error-codes
                    # 请求过于频繁
                    match status:
                        case 99991400:
                            print(f"Rate limit reached, exiting")
                            break
                    retry()
                except exceptions.ConnectionError as e:
                    print("Networking error, trying again")
                    retry()

            raise Exception("Max retries reached, failing.")
        return wrapper
    return decorator

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
    def __init__(self, value: Node | None = None, parent: 'DocTreeNode' = None, toc_entry: tuple[str,int] | None = None):
        self.__value = value
        self.__parent: DocTreeNode | None = parent
        self.__children: List[DocTreeNode] = []
        self.__toc_entry: tuple[str, int] | None = toc_entry

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
    def toc_entry(self):
        return self.__toc_entry

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
        self.__toc = []

    @property
    def toc(self):
        return self.__toc

    async def walk(self, space_id: str, parent_node: DocTreeNode | None = None, level: int = 1) -> None:
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
            @exponential_backoff()
            async def node_request():
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
                return resp

            resp = await node_request()

            # dive into current node
            subtree_tasks = []
            if resp.data.items:
                for item in resp.data.items:
                    logging.info(f'found document: title={item.title}, type={item.obj_type}')
                    node = DocTreeNode(item, toc_entry=(item.title, level))
                    parent_node.add_child(node)
                    subtree_task = asyncio.create_task(self.walk(space_id, node, level+1))
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

@exponential_backoff()
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

@exponential_backoff()
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
        # 详情见 https://open.larksuite.com/document/server-docs/docs/drive-v1/export_task/get
        if status == 0:
            return resp.data.result
        elif status != 1 and status != 2:
            logging.error(f'job failed: {node.title}: {status} {msg}')
            raise LarkOpenApiError(status, msg)

@exponential_backoff()
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


def generate_toc(nodes: List[DocTreeNode], tempdir) -> List[tuple[int, str, int]]:
    toc_no_page_numbers = list(map(lambda node: node.toc_entry, nodes))
    toc = []
    page_number = 1
    if COVER_PDF_PATH:
        with pymupdf.open(COVER_PDF_PATH) as cover_page:
            page_number += cover_page.page_count
    for index, node in enumerate(nodes):
        (title, level) = toc_no_page_numbers[index]
        path = os.path.join(tempdir, f'{index}.pdf')
        with pymupdf.open(path) as doc:
            # [lvl, title, page, dest]
            toc.append((level, title, page_number))
            page_number += doc.page_count

    return toc


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

    # generate toc
    toc = generate_toc(nodes, tempdir)

    # concatenate pdfs
    output_path = os.path.join(OUTPUT_DIR, 'guide.pdf')

    Path.unlink(output_path, missing_ok=True);
    with pymupdf.open() as doc:
        if COVER_PDF_PATH:
            doc.insert_file(COVER_PDF_PATH)
        for job in download_tasks:
            doc.insert_file(await job)
        doc.save(output_path)

    # incremental save is not allowed on a new document
    with pymupdf.open(output_path) as doc:
        doc.set_toc(toc)
        doc.saveIncr()

    # cleanup temporary directory
    shutil.rmtree(tempdir)


if __name__ == '__main__':
    asyncio.run(main())
