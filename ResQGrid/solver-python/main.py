import logging
from typing import List, Optional
from fastapi import FastAPI
from pydantic import BaseModel
from ortools.sat.python import cp_model

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("solver-python")

app = FastAPI(title="ResQGrid Optimization Solver")

# --- DATA MODELS ---

class IncidentModel(BaseModel):
    id: str
    type: str
    severity: int
    location: str
    description: Optional[str] = ""

class ResourceModel(BaseModel):
    id: str
    type: str
    capacity: int
    location: str
    status: str

class HospitalModel(BaseModel):
    id: str
    name: str
    location: str
    available_beds: int
    total_beds: int

class SolverRequest(BaseModel):
    incident: IncidentModel
    road_closures: List[str]
    resources: List[ResourceModel]
    hospitals: List[HospitalModel] = []

class RejectionLog(BaseModel):
    unit_id: str
    reason: str  # UNIT_RESERVED, ROUTE_INFEASIBLE, SPECIALTY_MISMATCH, COVERAGE_FLOOR

class HospitalRejectionLog(BaseModel):
    hospital_id: str
    reason: str   # HOSPITAL_FULL, HOSPITAL_UNREACHABLE

class SolverResponse(BaseModel):
    status: str
    selected_units: List[str]
    eta: float
    hospital_bed: str
    selected_hospital_id: str = ""
    explainability: List[RejectionLog]
    hospital_rejections: List[HospitalRejectionLog] = []

# --- UTILITIES ---

def get_sector_number(loc: str) -> int:
    for char in loc.split():
        if char.isdigit():
            return int(char)
    return 1

def calculate_travel_time(resource_loc: str, incident_loc: str) -> int:
    r_sec = get_sector_number(resource_loc)
    i_sec = get_sector_number(incident_loc)
    distance = abs(r_sec - i_sec)
    return int(5 + 3 * distance)

def calculate_hospital_distance(hospital_loc: str, incident_loc: str) -> int:
    h_sec = get_sector_number(hospital_loc)
    i_sec = get_sector_number(incident_loc)
    return abs(h_sec - i_sec)

def check_specialty_match(res_type: str, inc_type: str) -> bool:
    if res_type == "AIR":
        return True
    if inc_type == "MEDICAL" and res_type == "MEDICAL":
        return True
    if inc_type == "FIRE" and res_type == "FIRE":
        return True
    if inc_type == "FLOOD" and res_type == "WATER":
        return True
    return False

def get_hospital_display_name(hospital: HospitalModel, inc_type: str) -> str:
    if hospital.id:
        return f"{hospital.name} (Bed Reserved)"
    sec = get_sector_number(hospital.location if hospital else "Sector 1")
    if inc_type == "MEDICAL":
        return f"Mercy Trauma Center (Sector {sec})"
    elif inc_type == "FIRE":
        return f"City Burn ICU (Sector {sec})"
    else:
        return f"Emergency Evacuation Shelter (Sector {sec})"

def fallback_hospital_name(loc: str, inc_type: str) -> str:
    sec = get_sector_number(loc)
    if inc_type == "MEDICAL":
        return f"Mercy Trauma Center (Sector {sec})"
    elif inc_type == "FIRE":
        return f"City Burn ICU (Sector {sec})"
    else:
        return f"Emergency Evacuation Shelter (Sector {sec})"

# --- SOLVER ROUTE ---

@app.post("/solve", response_model=SolverResponse)
def solve(req: SolverRequest):
    logger.info(
        f"Joint hospital+vehicle optimization: Incident {req.incident.id} "
        f"(Type: {req.incident.type}, Severity: {req.incident.severity})"
    )

    incident = req.incident
    road_closures = req.road_closures
    resources = req.resources
    hospitals = req.hospitals

    # ---------------------------------------------------------------
    # STAGE 1: Pre-filter resources (hard constraint checking)
    # ---------------------------------------------------------------
    explainability: List[RejectionLog] = []
    candidate_units: List[ResourceModel] = []

    for res in resources:
        # 1. UNIT_RESERVED
        if res.status == "busy":
            explainability.append(RejectionLog(unit_id=res.id, reason="UNIT_RESERVED"))
            continue

        # 2. ROUTE_INFEASIBLE
        is_blocked = any(
            closure.lower() in res.location.lower() or closure.lower() in incident.location.lower()
            for closure in road_closures
        )
        if is_blocked:
            explainability.append(RejectionLog(unit_id=res.id, reason="ROUTE_INFEASIBLE"))
            continue

        # 3. SPECIALTY_MISMATCH
        if not check_specialty_match(res.type, incident.type):
            explainability.append(RejectionLog(unit_id=res.id, reason="SPECIALTY_MISMATCH"))
            continue

        candidate_units.append(res)

    # ---------------------------------------------------------------
    # STAGE 2: Pre-filter hospitals (only for MEDICAL / FIRE)
    # ---------------------------------------------------------------
    hospital_rejections: List[HospitalRejectionLog] = []
    candidate_hospitals: List[HospitalModel] = []
    needs_bed = incident.type in ("MEDICAL", "FIRE")

    if needs_bed and hospitals:
        for h in hospitals:
            # Check if incident sector is blocked (hospital unreachable)
            incident_sector_blocked = any(
                closure.lower() in incident.location.lower() for closure in road_closures
            )
            if incident_sector_blocked:
                hospital_rejections.append(HospitalRejectionLog(hospital_id=h.id, reason="HOSPITAL_UNREACHABLE"))
                continue
            if h.available_beds <= 0:
                hospital_rejections.append(HospitalRejectionLog(hospital_id=h.id, reason="HOSPITAL_FULL"))
                continue
            candidate_hospitals.append(h)

    # ---------------------------------------------------------------
    # STAGE 3: Build CP-SAT model — joint vehicle + hospital optimization
    # ---------------------------------------------------------------
    model = cp_model.CpModel()

    # --- Vehicle dispatch variables ---
    x = {}
    for i, res in enumerate(candidate_units):
        x[i] = model.NewBoolVar(f"dispatch_{res.id}")

    # Constraint: total dispatched capacity >= incident severity
    if candidate_units:
        model.Add(
            sum(x[i] * res.capacity for i, res in enumerate(candidate_units)) >= incident.severity
        )

    # Travel times for vehicles
    travel_times = [calculate_travel_time(res.location, incident.location) for res in candidate_units]

    # --- Hospital selection variables (only if bed needed and candidates exist) ---
    y = {}
    hospital_distances = []
    if needs_bed and candidate_hospitals:
        for j, h in enumerate(candidate_hospitals):
            y[j] = model.NewBoolVar(f"hospital_{h.id}")
            hospital_distances.append(calculate_hospital_distance(h.location, incident.location))

        # Constraint: exactly one hospital selected
        model.Add(sum(y[j] for j in range(len(candidate_hospitals))) == 1)

    # --- Joint Objective: minimize vehicle travel + hospital distance (weighted) ---
    vehicle_obj = sum(x[i] * travel_times[i] for i in range(len(candidate_units)))
    hospital_obj = (
        sum(y[j] * hospital_distances[j] for j in range(len(candidate_hospitals)))
        if (needs_bed and candidate_hospitals) else 0
    )
    # Weight: vehicle time (×3) is more critical than hospital distance (×1)
    model.Minimize(3 * vehicle_obj + hospital_obj)

    # --- Solve ---
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 2.0
    status = solver.Solve(model)

    selected_units: List[str] = []
    selected_hospital_id = ""
    hospital_bed_name = fallback_hospital_name(incident.location, incident.type)
    eta = 15.0

    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        # Collect dispatched vehicles
        selected_times = []
        for i, res in enumerate(candidate_units):
            if solver.Value(x[i]) == 1:
                selected_units.append(res.id)
                selected_times.append(travel_times[i])
            else:
                explainability.append(RejectionLog(unit_id=res.id, reason="COVERAGE_FLOOR"))

        # Collect selected hospital
        if needs_bed and candidate_hospitals:
            for j, h in enumerate(candidate_hospitals):
                if solver.Value(y[j]) == 1:
                    selected_hospital_id = h.id
                    hospital_bed_name = get_hospital_display_name(h, incident.type)
                    break

        # Compute risk-weighted ETA
        if selected_times:
            base_eta = max(selected_times)
            risk_multiplier = 1.0
            for closure in road_closures:
                if f"sector {get_sector_number(incident.location)}" in closure.lower():
                    risk_multiplier += 0.30
                elif any(
                    f"sector {get_sector_number(res.location)}" in closure.lower()
                    for res in candidate_units if res.id in selected_units
                ):
                    risk_multiplier += 0.15
            eta = round(base_eta * risk_multiplier, 1)

        logger.info(
            f"Optimal dispatch: units={selected_units}, hospital={selected_hospital_id}, "
            f"risk-weighted ETA={eta}m"
        )

        return SolverResponse(
            status="OPTIMAL",
            selected_units=selected_units,
            eta=eta,
            hospital_bed=hospital_bed_name,
            selected_hospital_id=selected_hospital_id,
            explainability=explainability,
            hospital_rejections=hospital_rejections,
        )

    else:
        logger.warning("Solver infeasible: insufficient capacity to meet severity constraint.")
        for res in candidate_units:
            explainability.append(RejectionLog(unit_id=res.id, reason="COVERAGE_FLOOR"))

        return SolverResponse(
            status="INFEASIBLE",
            selected_units=[],
            eta=0.0,
            hospital_bed="No Beds Reserved (Infeasible)",
            selected_hospital_id="",
            explainability=explainability,
            hospital_rejections=hospital_rejections,
        )


@app.get("/health")
def health():
    return {"status": "healthy", "service": "solver-python"}
