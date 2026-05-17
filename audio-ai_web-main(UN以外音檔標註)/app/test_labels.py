from app import create_app
from app.models import Label, CetaceanInfo
app = create_app()
app.app_context().push()

labels = Label.query.all()
print("Labels:", {l.id: l.name for l in labels}, type(labels[0].id) if labels else None)

c = CetaceanInfo.query.filter(CetaceanInfo.event_type != 0).first()
if c:
    print("First cetacean event_type:", c.event_type, type(c.event_type))
else:
    print("No cetaceans found")
