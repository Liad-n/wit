from datetime import datetime, timedelta, timezone
from filecmp import dircmp
from fnmatch import filter
from itertools import product
import logging
import os
from pathlib import Path, PurePath
import random
from shutil import copy, copyfile, copytree, ignore_patterns, rmtree
import string
import sys

from graphviz import Digraph


class NoWitFolderFoundError(Exception):
    pass


class NotEnoughArgumentsError(Exception):
    pass


class WitCommandNotFoundError(Exception):
    pass


class NonExistentCommitIdError(Exception):
    pass


class UnableToCheckoutError(Exception):
    pass


class NoPreviousCommitsError(Exception):
    pass


class BranchExistsError(Exception):
    pass


class InvalidMergeError(Exception):
    pass


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

LOG_FORMAT = "%(levelname)s  %(asctime)s - %(message)s"
formatter = logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")

file_handler = logging.FileHandler(Path(__file__).parent / 'wit.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def init(path=None):
    if path is None:
        path = os.getcwd()
    folders_to_create = ('.wit', Path('.wit', 'images'),
                         Path('.wit', 'staging_area'))
    exists = False
    for folder in folders_to_create:
        if not os.path.exists(folder):
            os.mkdir(folder)
        else:
            exists = True
    Path('.wit', 'activated.txt').write_text('master')
    if not exists:
        logger.info(f'Wit repository initialized in {path}')
    else:
        logger.info(f'Wit repository already exists in {path}')


def evaluate_args():
    try:
        command, args = sys.argv[1], sys.argv[2:]
        if command == 'init':
            init()
        elif command == 'add':
            path_to_add = args[0]
            add(path_to_add)
        elif command == 'commit':
            commit_msg = args[0]
            commit(commit_msg)
        elif command == 'status':
            status()
        elif command == 'checkout':
            commit_id = args[0]
            checkout(commit_id)
        elif command == 'graph':
            graph()
        elif command == 'branch':
            branch_name = args[0]
            branch(branch_name)
        elif command == 'merge':
            branch_to_merge = args[0]
            merge(branch_to_merge)
        else:
            raise WitCommandNotFoundError(
                f"The following command doesn't exist: {command}"
            )
    except IndexError:
        err = (
            '\nwit must receive a command in order to work.'
            '\nfor example - wit init or wit add <filename>'
        )
        raise NotEnoughArgumentsError(err)


def find_wit():
    path = Path.cwd()
    found_wit = False
    if Path('.wit').exists():
        found_wit = True
        return path
    while not found_wit and path != path.parent:
        path = path.parent
        os.chdir(path)
        if Path('.wit').exists():
            found_wit = True
            return path
    raise NoWitFolderFoundError(
        'No .wit folder found, unable to process request.\n'
        'Try the init command to create a .wit folder.'
    )


def add(path_to_add):
    wit_root = find_wit()
    dst = PurePath(wit_root, '.wit', 'staging_area', path_to_add)
    try:
        copytree(path_to_add, dst)
    except NotADirectoryError:
        if not os.path.exists(dst.parent):
            os.makedirs(dst.parent)
        copy(path_to_add, dst)
    except FileExistsError:
        if not os.path.exists(dst.parent):
            copyfile(path_to_add, dst)
        logger.info(f'{path_to_add} is already staged.')


def commit(message, second_parent_for_merge=None):
    wit_root = find_wit()
    commit_id = gen_hash()
    wit_images_commit_id = Path(wit_root, '.wit', 'images', commit_id)
    wit_staging = Path(wit_root, '.wit', 'staging_area')
    copytree(wit_staging, Path(wit_images_commit_id))
    gen_commit_txt(commit_id, message, second_parent_for_merge)
    gen_references(commit_id)
    logger.info(f'Committed successfully commit id: {commit_id}')

    return commit_id


def get_status(wit_root, references=None):
    if references is None:
        references = read_references()
    changes_to_be_committed = get_changes_to_be_committed(
        wit_root, references['HEAD']
    )
    changes_not_staged = get_changes_not_staged(wit_root)
    untracked = get_untracked(wit_root)

    return {'changes_to_be_committed': changes_to_be_committed,
            'changes_not_staged': changes_not_staged,
            'untracked': untracked}


def status():
    wit_root = find_wit()
    ref = read_references()
    if ref:
        current_status = get_status(wit_root, ref)
        message = (
            f"HEAD: {ref['HEAD']}\n"
            f"Changes to be committed:\n {current_status['changes_to_be_committed']}\n\n"
            f"Changes not staged for commit:\n {current_status['changes_not_staged']}\n\n"
            f"Untracked files:\n {current_status['untracked']}\n\n"
        )
        logger.info(message)
    else:
        raise NoPreviousCommitsError(
            'No previous commits have ever been taken.')


def checkout(commit_id):
    wit_root = find_wit()
    branch_name = ''
    if len(commit_id) != 40:
        branch_name = commit_id
        commit_id = find_commit_by_branch_name(branch_name)
    commit_id_exists = find_commit_by_id(wit_root, commit_id)
    if not (commit_id_exists):
        raise NonExistentCommitIdError('The commit ID given does not exist.')

    current_status = get_status(wit_root)
    changes_to_be_committed = current_status['changes_to_be_committed']
    changes_not_staged = current_status['changes_not_staged']
    untracked = current_status['untracked']

    status_ok = not (changes_to_be_committed or changes_not_staged)
    if not status_ok:
        raise UnableToCheckoutError(
            'You cannot checkout with files to be committed or changes not staged for commit.'
        )
    logger.info(f'wit restoring commit: {commit_id}')
    path_to_copy = Path(wit_root, '.wit', 'images', f'{commit_id}')
    copytree(path_to_copy, wit_root, ignore=ignore_patterns(
        *untracked, '.wit'), dirs_exist_ok=True)
    staging_area = Path(wit_root, '.wit', 'staging_area')
    rmtree(staging_area)
    staging_area.mkdir()
    copytree(path_to_copy, staging_area, dirs_exist_ok=True)
    edit_references('HEAD', commit_id)

    activated_text = f'{branch_name}'
    Path(wit_root, '.wit', 'activated.txt').write_text(activated_text)
    return commit_id


def draw_graph(commit_list):
    enumerated_commits = enumerate(commit_list)
    dot = Digraph(name='.wit/witgraph', comment='Wit Graph', format="png",
                  node_attr={'color': 'lightblue', 'style': 'filled', 'shape': 'circle'})
    for i, commit_id in enumerated_commits:
        formatted_commit_id = commit_id[:20] + '\n' + commit_id[20:]
        dot.node(str(i), formatted_commit_id)
    input_for_edges = [f'{i}{i + 1}' for i in range(len(commit_list) - 1)]
    dot.edges(input_for_edges)

    dot.view()


def graph():
    wit_root = find_wit()
    ref = read_references()
    if ref:
        head_id = ref['HEAD']
        commits = list(get_all_parent_commits(
            wit_root, head_id, flat=True))[::-1]
        print(commits)
        draw_graph(commits)
    else:
        raise NoPreviousCommitsError(
            'No previous commits have ever been taken.')


def branch(branch_name):
    wit_root = find_wit()
    ref = read_references()
    if branch_name not in ref:
        text_to_add = f'{branch_name}={ref["HEAD"]}'
        with Path(wit_root, '.wit', 'references.txt').open('a') as fh:
            fh.write(text_to_add)
    else:
        raise BranchExistsError(
            'Branch name exists, cannot create branch with this name.')


# Based on https://stackoverflow.com/questions/42487578/python-shutil-copytree-use-ignore-function-to-keep-specific-files-types
def include_patterns(*patterns):
    def _ignore_patterns(path, names):
        keep = {name for pattern in patterns
                for name in filter(names, pattern)}
        ignore = {name for name in names
                  if name not in keep and not Path(path, name).is_dir()}
        return ignore
    return _ignore_patterns


def merge(branch_to_merge):
    wit_root = find_wit()
    current_branch = get_activated_branch(wit_root)
    commit_a, commit_b = find_commit_by_branch_name(
        current_branch), find_commit_by_branch_name(branch_to_merge)
    print(commit_b, commit_a)
    if commit_a == commit_b:
        raise InvalidMergeError('Branches are already on the same commit.')

    common_parent = get_common_parent(commit_a, commit_b)
    staging_area = Path(wit_root, '.wit', 'staging_area')
    images_folder = Path(wit_root, '.wit', 'images')

    dcmp1 = dircmp(Path(images_folder, common_parent),
                   Path(images_folder, commit_b))
    dcmp2 = dircmp(staging_area, Path(images_folder, commit_a))
    changes_branch_to_merge_with_parent = list(
        get_changes(dcmp1, left_only=False, right_only=True))
    changes_active_branch_with_staging = list(
        get_changes(dcmp2, left_only=True, right_only=True))
    if changes_active_branch_with_staging:
        raise InvalidMergeError(
            'Merging is not possible due to file differences.')
    copytree(Path(images_folder, commit_b), staging_area, ignore=include_patterns(
        *changes_branch_to_merge_with_parent), dirs_exist_ok=True)
    commit(
        f'MERGED {current_branch} with {branch_to_merge}!', second_parent_for_merge=commit_b)


def get_common_parent(commit_a, commit_b, wit_root=None):
    if wit_root is None:
        wit_root = find_wit()

    parents_a = get_all_parent_commits(wit_root, commit_a, flat=True)
    parents_b = get_all_parent_commits(wit_root, commit_b, flat=True)
    for a, b in product(parents_a, parents_b):
        if a == b:
            return a


def get_activated_branch(wit_root=None):
    if wit_root is None:
        wit_root = find_wit()
    return Path(wit_root, '.wit', 'activated.txt').read_text()


def txt_to_dict(path):
    path = Path(path)
    if os.path.exists(path) and path.is_file():
        with open(Path(path), 'r') as fh:
            f_content = fh.read()
    else:
        f_content = ''
    if f_content:
        data_lists = (line.split('=') for line in f_content.splitlines())
        return {k: v for k, v in data_lists}
    else:
        return {}


def get_parent_commit(wit_root, commit_id):
    if not commit_id:
        return ''

    commit_text_path = Path(wit_root, '.wit', 'images', f'{commit_id}.txt')
    commit_text_dict = txt_to_dict(commit_text_path)
    parent = commit_text_dict.get('parent', '')
    if ',' not in parent:
        return parent
    else:
        return parent.split(',')


def get_all_parent_commits(wit_root, commit_id, flat=False):
    yield commit_id
    parent = get_parent_commit(wit_root, commit_id)
    while parent and parent != 'None':
        if flat:
            if isinstance(parent, list):
                yield parent[0]
                yield parent[1]
        else:
            yield parent
        parent = get_parent_commit(wit_root, parent)


def find_commit_by_id(wit_root, commit_id):
    images = Path(wit_root, '.wit', 'images')
    return commit_id in os.listdir(images)


def find_commit_by_branch_name(branch_name):
    ref = read_references()
    if ref:
        commit_id = ref.get(branch_name)
        return commit_id
    return None


def gen_timestamp():
    return datetime.now(timezone(timedelta(hours=3))).strftime("%a %b %d %H:%M:%S %Y %z")


def gen_commit_txt(commit_id, message, second_parent_for_merge=None):
    timestamp = gen_timestamp()
    parent = None
    ref = read_references()
    if ref:
        parent = ref['HEAD']
    if second_parent_for_merge is not None:
        parent += f',{second_parent_for_merge}'
    content = (
        f'parent={parent}\n'
        f'date={timestamp}\n'
        f'message={message}'
    )
    with open(Path('.wit', 'images', f'{commit_id}.txt'), 'w') as fh:
        fh.write(content)


def read_references():
    return txt_to_dict(Path('.wit', 'references.txt'))


def edit_references(ref_name, ref_id):
    ref = read_references()
    ref[ref_name] = ref_id
    content = ''
    for r_name, r_id in ref.items():
        line = f'{r_name}={r_id}\n'
        content += line
    with open(Path('.wit', 'references.txt'), 'r+') as fh:
        fh.write(content)


def gen_references(commit_id):
    ref = read_references()
    active_branch = get_activated_branch()
    head_id = commit_id
    if ref:
        curr_head_id = ref['HEAD']
        active_id = ref[active_branch]
        if curr_head_id == active_id:
            active_id = commit_id
        edit_references('HEAD', head_id)
        edit_references(active_branch, active_id)
    else:
        content = (
            f'HEAD={head_id}\n'
            f'{active_branch}={commit_id}\n'
        )
        with open(Path('.wit', 'references.txt'), 'w') as fh:
            fh.write(content)


def gen_hash():
    h = ''.join(random.choices(string.ascii_letters[:6] + string.digits, k=40))

    return h


def get_changes(dcmp, left_only=False, right_only=False, diff=True):
    if diff:
        for name in dcmp.diff_files:
            yield name
    if left_only:
        for name in dcmp.left_only:
            yield name
    if right_only:
        for name in dcmp.right_only:
            yield name

    for sub_dcmp in dcmp.subdirs.values():
        yield from get_changes(sub_dcmp, left_only, right_only, diff)


def get_changes_to_be_committed(wit_root, last_commit_id):
    staging_area = Path(wit_root, '.wit', 'staging_area')
    last_commit_folder = Path(wit_root,
                              '.wit', 'images', last_commit_id)
    dcmp = dircmp(staging_area, last_commit_folder, ignore=['.wit'])
    return list(get_changes(dcmp, left_only=True, right_only=False, diff=True))


def get_changes_not_staged(wit_root):
    staging_area = Path(wit_root, '.wit', 'staging_area')
    dcmp = dircmp(wit_root, staging_area, ignore=['.wit'])
    return list(get_changes(dcmp, left_only=False, right_only=False, diff=True))


def get_untracked(wit_root):
    staging_area = Path(wit_root, '.wit', 'staging_area')
    dcmp = dircmp(wit_root, staging_area, ignore=['.wit'])
    return list(get_changes(dcmp, left_only=True, diff=False))


if __name__ == "__main__":
    try:
        evaluate_args()
    except (
        NoWitFolderFoundError,
        NotEnoughArgumentsError,
        WitCommandNotFoundError,
        NonExistentCommitIdError,
        UnableToCheckoutError,
        NoPreviousCommitsError,
        BranchExistsError,
        InvalidMergeError
    ) as err:
        logger.warning(f'wit: {err}')
    except Exception as err:
        logger.error(err)
