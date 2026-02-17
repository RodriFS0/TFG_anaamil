import subprocess
from pathlib import Path

def ensure_processed_data_updated(
    repo_url: str = "https://github.com/michaelwitting/RepoRT.git",
    branch: str = "master",
    subdir: str = "processed_data",
    local_repo_dir: str = "external/RepoRT_remote",
) -> Path:
    """
    Sincroniza SOLO processed_data/ desde GitHub usando sparse-checkout.
    Devuelve la ruta local a processed_data/.
    """
    local_repo = Path(local_repo_dir)
    local_repo.parent.mkdir(parents=True, exist_ok=True)

    if not local_repo.exists():
        # clone mínimo
        subprocess.check_call(["git", "clone", "--filter=blob:none", "--no-checkout", "-b", branch, repo_url, str(local_repo)])
        subprocess.check_call(["git", "-C", str(local_repo), "sparse-checkout", "init", "--cone"])
        subprocess.check_call(["git", "-C", str(local_repo), "sparse-checkout", "set", subdir])
        subprocess.check_call(["git", "-C", str(local_repo), "checkout"])
    else:
        # update "exacto" a lo último del branch
        subprocess.check_call(["git", "-C", str(local_repo), "fetch", "origin", branch, "--depth", "1"])
        subprocess.check_call(["git", "-C", str(local_repo), "reset", "--hard", f"origin/{branch}"])
        # asegurar que sparse-checkout sigue apuntando al subdir
        subprocess.check_call(["git", "-C", str(local_repo), "sparse-checkout", "set", subdir])

    return local_repo / subdir
