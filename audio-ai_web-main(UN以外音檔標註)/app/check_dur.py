from app import create_app
from app.models import AudioInfo
app = create_app()
app.app_context().push()

audios = AudioInfo.query.all()
for a in audios:
    print(f"Audio {a.id}: file={a.file_name}, duration={a.record_duration}")
