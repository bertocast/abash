from __future__ import annotations

import anyio

from abash import Bash


async def main() -> None:
    script = """
    GREETING=hello TARGET=world echo $GREETING > /workspace/demo.txt
    cat < /workspace/demo.txt | cat
    echo /workspace/*.txt
    echo pear > /workspace/fruit.txt
    echo apple >> /workspace/fruit.txt
    echo pear >> /workspace/fruit.txt
    echo banana >> /workspace/fruit.txt
    cat /workspace/fruit.txt | sort | uniq
    echo bert,eng,core > /workspace/people.csv
    echo ana,pm,product >> /workspace/people.csv
    cat /workspace/people.csv | tr , : | cut -d : -f 1,3 | head -n 1
    echo bert > /workspace/names.txt
    echo core > /workspace/teams.txt
    paste -d , /workspace/names.txt /workspace/teams.txt | sed s/core/platform/
    env -i OWNER=berto printenv OWNER
    export TEAM=core
    printenv TEAM
    alias ll='ls -l'
    ll /workspace | head -n 1
    unalias ll
    expr 2 + 3
    time echo timed
    timeout 0.01 sleep 0.001
    whoami
    hostname
    which rg dirname
    dirname /workspace/people.csv
    basename /workspace/people.csv
    tree -L 2 /workspace | head -n 5
    stat /workspace/people.csv
    file /workspace/people.csv
    echo bert,1 > /workspace/roster.csv
    echo ana,2 >> /workspace/roster.csv
    echo core,1 > /workspace/orgs.csv
    echo growth,2 >> /workspace/orgs.csv
    join -t , -1 2 -2 2 /workspace/roster.csv /workspace/orgs.csv
    awk -F , '$2 == "growth" { print $1, NR, FNR, NF }' /workspace/orgs.csv
    find /workspace -name "*.csv" -type f | sort
    ls -l /workspace | head -n 2
    printf 'abc\ndef\n' | tac | rev
    printf 'left\nright\n' | nl -ba
    printf 'bcdef\n' | strings -n 3
    printf 'abcdef' | fold -w 3
    printf 'a\tb\n' | expand -t 4 | unexpand -a -t 4
    printf 'name role\nbert eng\n' | column -t | tail -n 1
    printf 'alpha common\nbeta common\n' | xargs -n 2 echo
    cp /workspace/demo.txt /workspace/demo-copy.txt
    mv /workspace/demo-copy.txt /workspace/demo-final.txt
    echo cab | tee /workspace/report.txt | tr abc xyz
    printf '%s\n' bert
    seq 1 3
    date +%F
    rg growth /workspace
    comm -3 /workspace/names.txt /workspace/teams.txt
    diff /workspace/names.txt /workspace/teams.txt || true
    split -l 1 /workspace/names.txt /workspace/name-
    od -An -tx1 /workspace/name-aa
    base64 /workspace/name-aa | base64 -d
    md5sum /workspace/name-aa
    sha1sum /workspace/name-aa
    sha256sum /workspace/name-aa
    bash -c 'export INNER=child; printenv INNER'
    sh -c 'echo shell-child'
    rm /workspace/fruit.txt
    if false; then echo broken; else echo "${TARGET} recovered"; fi
    false || echo fallback
    """

    async with Bash() as bash:
        result = await bash.exec_script(script)

    print("stdout:", repr(result.stdout))
    print("metadata:", result.metadata)


if __name__ == "__main__":
    anyio.run(main)
