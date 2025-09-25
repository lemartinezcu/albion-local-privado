import sys
from . import elementary_volumes, to_obj

with open(sys.argv[1]) as f:
    cell_id_ = f.readline().rstrip()
    graph_id_ = f.readline().rstrip()
    geom_ = f.readline().rstrip()
    holes_ = f.readline().rstrip().split()
    starts_ = f.readline().rstrip().split()
    ends_ = f.readline().rstrip().split()
    hole_ids_ = f.readline().rstrip().split()
    node_ids_ = f.readline().rstrip().split()
    nodes_ = f.readline().rstrip().split()
    end_ids_ = f.readline().rstrip().split()
    end_geoms_ = f.readline().rstrip().split()
    end_holes_ = f.readline().rstrip().split()
    idx = 0
    for v, f1, f2, f3 in elementary_volumes(
        holes_,
        starts_,
        ends_,
        hole_ids_,
        node_ids_,
        nodes_,
        end_ids_,
        end_geoms_,
        end_holes_,
    ):
        open("/tmp/volume%d.obj" % (idx), "w").write(to_obj(v))
        open("/tmp/face1_%d.obj" % (idx), "w").write(to_obj(f1))
        open("/tmp/face2_%d.obj" % (idx), "w").write(to_obj(f2))
        open("/tmp/face3_%d.obj" % (idx), "w").write(to_obj(f3))
        idx += 1
