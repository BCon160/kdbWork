//// u.q (edited for Advanced kdb+ exam) ////
//Author: Brendan Connolly
//Description: Normal u.q with extra features added such as logging in line with Advanced kdb+ CMTP.
//Note: Any code I write won't follow the same terse format as the normal u.q code!
//All double slashes below are edits that I have made

\d .u
init:{
    w::t!(count t::tables`.)#();
    //Initialise the table update count dictionary
    tCount::w
 }

del:{
    w[x]_:w[x;;0]?y
 };
 
.z.pc:{
    del[;x]each t
 };

sel:{
    $[`~y;
        x;
        select from x where sym in y
    ]
 }

pub:{[t;x]
    {[t;x;w]
        if[count x:sel[x]w 1;
            (neg first w)(`upd;t;x)
        ]
    }[t;x]each w t
 }

add:{
    $[(count w x)>i:w[x;;0]?.z.w;
        .[`.u.w;(x;i;1);union;y];
        w[x],:enlist(.z.w;y)
    ];
    (x; 
    $[99=type v:value x;
        sel[v]y;
        0#v
    ])
 }

sub:{
    if[x~`;
        :sub[;y]each t
    ];
    //Allow subscriptions to more than 1 table (but not nessesarily all)
    if[1 < count x;
        :sub[;y]each x
    ];
    if[not x in t;
        'x
    ];
    del[x].z.w;
    add[x;y]
 }

end:{
    (neg union/[w[;;0]])@\:(`.u.end;x)
 }
