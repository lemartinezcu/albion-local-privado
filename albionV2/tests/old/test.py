#!/usr/bin/env python3

if __name__ == "__main__":
    from albion.project import Project
    import os

    PROJECT_NAME = "niger"
    SRID = 32632
    DATA_DIR = "../../../albion_data"

    if False:  # new project
        if Project.exists(PROJECT_NAME):
            Project.delete(PROJECT_NAME)
        project = Project.create(PROJECT_NAME, SRID)
        project.import_data(DATA_DIR)
    else:
        project = Project(PROJECT_NAME)
        project.update()

    project.execute_script(os.path.join(os.path.dirname(__file__), "test.sql"))

    with project.connect() as con:
        cur = con.cursor()
        cur.execute(
            """
        select (t.r).from_, (t.r).to_, (t.r).oc, (t.r).accu, (t.r).grade
        from (
            select albion.segmentation(array_agg(gamma order by from_),
            array_agg(from_ order by from_), array_agg(to_ order by from_),
            1., 1., 10, min(from_)) as r
            from _albion.radiometry
            where hole_id='GART_0556_1'
            ) as t
            """
        )
        for rec in cur.fetchall():
            print(rec)
